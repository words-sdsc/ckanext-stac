from __future__ import unicode_literals
import uuid
from dateutil.parser import parse
from simplejson.scanner import JSONDecodeError
from ckan import model
from ckanext.harvest.harvesters.base import HarvesterBase
from ckan.plugins.core import implements
import ckan.plugins.toolkit as toolkit
from ckanext.harvest.interfaces import IHarvester
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
#import pystac
import json
import requests
from ckan.lib.munge import munge_title_to_name, munge_tag
from urlparse import urlparse
import logging
import random
import collections
log = logging.getLogger(__name__)

class StacHarvester(HarvesterBase):
    '''
    A SpatioTemporal Asset Catalog (STAC) harvester
    '''
    implements(IHarvester)

    def info(self):
        
        return {
                'name': 'stac',
                'title': 'SpatioTemporal Asset Catalog (STAC) harvester',
                'description': 'A harvester for SpatioTemporal Asset Catalogs'
            }

    def _delete_dataset(self, id):
        base_context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True
        }
        # Delete package
        toolkit.get_action('package_delete')(base_context, {'id': id})
        log.info('Deleted package with id {0}'.format(id))

    def _get_existing_dataset(self, guid):
        '''
        Check if a dataset with an `identifier` extra already exists.

        Return a dict in `package_show` format.
        '''
        datasets = model.Session.query(model.Package.id) \
            .join(model.PackageExtra) \
            .filter(model.PackageExtra.key == 'identifier') \
            .filter(model.PackageExtra.value == guid) \
            .filter(model.Package.state == 'active') \
            .all()

        if not datasets:
            return None
        elif len(datasets) > 1:
            log.error('Found more than one dataset with the same guid: {0}'
                      .format(guid))

        return toolkit.get_action('package_show')({}, {'id': datasets[0][0]})

    def _get_object_extra(self, harvest_object, key):
        '''
        Helper function for retrieving the value from a harvest object extra,
        given the key
        '''
        for extra in harvest_object.extras:
            if extra.key == key:
                return extra.value
        return None

    def _get_package_extra(self, pkg_dict, key):
        '''
        Helper function to retrieve the value from a package dict extra, given
        the key.
        '''
        for extra in pkg_dict.get('extras', []):
            if extra.get('key') == key:
                return extra.get('value')
        return None

    def _mark_datasets_for_deletion(self, guids_in_source, harvest_job):
        '''
        Given a list of guids in the remote source, check which in the DB need
        to be deleted. Query all guids in the DB for this source and calculate
        the difference. For each of these create a HarvestObject with the
        dataset id, marked for deletion.

        Return a list with the ids of the Harvest Objects to delete.
        '''

        object_ids = []

        # Get all previous current guids and dataset ids for this source
        query = \
            model.Session.query(HarvestObject.guid, HarvestObject.package_id) \
            .filter(HarvestObject.current == True) \
            .filter(HarvestObject.harvest_source_id == harvest_job.source.id)  # noqa

        guid_to_package_id = {}
        for guid, package_id in query:
            guid_to_package_id[guid] = package_id

        guids_in_db = guid_to_package_id.keys()

        # Get objects/datasets to delete (ie in the DB but not in the source)
        guids_to_delete = set(guids_in_db) - set(guids_in_source)

        # Create a harvest object for each of them, flagged for deletion
        for guid in guids_to_delete:
            obj = HarvestObject(guid=guid, job=harvest_job,
                                package_id=guid_to_package_id[guid],
                                extras=[HarvestObjectExtra(key='status',
                                                           value='delete')])

            # Mark the rest of objects for this guid as not current
            model.Session.query(HarvestObject) \
                         .filter_by(guid=guid) \
                         .update({'current': False}, False)
            obj.save()
            object_ids.append(obj.id)

        return object_ids

    def _build_package_dict(self, context, harvest_object):
        '''
        Build and return a package_dict suitable for use with CKAN
        `package_create` and `package_update`.
        '''

        # Local harvest source organization
        source_dataset = toolkit.get_action('package_show')(
            context.copy(),
            {'id': harvest_object.source.id}
        )
        local_org = source_dataset.get('owner_org')

        #local_org = urlparse(harvest_job.source.org).hostname

        res = json.loads(harvest_object.content)

        package_dict = {
            'title': res['title'],
            'name': self._gen_new_name(res['name']),
            'url': res['url'],
            'notes': res['notes'],
            
            'tags': [],
            'extras': res['extras'],
            'identifier': res['id'],
            'license_id': res['license_id'],
            'owner_org': local_org,
            'resources': res['resources']
        }
       

        # Add tags
        package_dict['tags'] = [{'name': munge_tag(t)} for t in res['tags']]

        return package_dict

    def process_package(self, package, harvest_object):
        '''
        Subclasses can override this method to perform additional processing on
        package dicts during import_stage.
        '''
        return package
    
    
    def validate_config(self, config):
        '''

        [optional]

        Harvesters can provide this method to validate the configuration
        entered in the form. It should return a single string, which will be
        stored in the database.  Exceptions raised will be shown in the form's
        error messages.

        :param harvest_object_id: Config string coming from the form
        :returns: A string with the validated configuration options
        '''

    def get_original_url(self, harvest_object_id):
        '''

        [optional]

        This optional but very recommended method allows harvesters to return
        the URL to the original remote document, given a Harvest Object id.
        Note that getting the harvest object you have access to its guid as
        well as the object source, which has the URL.
        This URL will be used on error reports to help publishers link to the
        original document that has the errors. If this method is not provided
        or no URL is returned, only a link to the local copy of the remote
        document will be shown.

        Examples:
            * For a CKAN record: http://{ckan-instance}/api/rest/{guid}
            * For a WAF record: http://{waf-root}/{file-name}
            * For a CSW record: http://{csw-server}/?Request=GetElementById&Id={guid}&...

        :param harvest_object_id: HarvestObject id
        :returns: A string with the URL to the original document
        '''

    def gather_stage(self, harvest_job):
        '''
        The gather stage will receive a HarvestJob object and will be
        responsible for:
            - gathering all the necessary objects to fetch on a later.
            stage (e.g. for a CSW server, perform a GetRecords request)
            - creating the necessary HarvestObjects in the database, specifying
            the guid and a reference to its job. The HarvestObjects need a
            reference date with the last modified date for the resource, this
            may need to be set in a different stage depending on the type of
            source.
            - creating and storing any suitable HarvestGatherErrors that may
            occur.
            - returning a list with all the ids of the created HarvestObjects.
            - to abort the harvest, create a HarvestGatherError and raise an
            exception. Any created HarvestObjects will be deleted.

        :param harvest_job: HarvestJob object
        :returns: A list of HarvestObject ids
        '''
        
        def bbox_to_polygon(bbox):
    
            lon1=bbox[0]
            lon2=bbox[2]
            lat1=bbox[3]
            lat2=bbox[1]
            polygon = [[lon1,lat1],[lon2,lat1],[lon2,lat2],[lon1,lat2]]
            polygon.append(polygon[0])  #repeat the first point to create a 'closed loop'
            
            return json.dumps({"type".encode('utf-8'): "Polygon".encode('utf-8'), "coordinates".encode('utf-8'): [polygon]})
        
        def convert_unicode_to_str(data):
            if isinstance(data, basestring):
                return str(data)
            elif isinstance(data, collections.Mapping):
                return dict(map(convert, data.iteritems()))
            elif isinstance(data, collections.Iterable):
                return type(data)(map(convert, data))
            else:
                return data
        
        def get_cfo_data(domain):
            #domain = 'https://storage.googleapis.com/cfo-public/vegetation/collection.json'
            collection = requests.get(domain)
            collection_result = collection.json()
            
            # extract the dataset "items" and add metadata fields like extent, description, etc
            all_data=[]
            for dataset in collection_result['links']:
                if dataset['rel']=='item':
                    result_ = requests.get(dataset['href'])
                    output=result_.json()
                    
                    output['description']=collection_result['description']
                    output['tags']=collection_result['keywords']
                    output['extent']=collection_result['extent']
                    output['license']=collection_result['license']
                    output['providers']=collection_result['providers']
                
                    all_data.append(output)
                    
                    # create ckan dataJSON by creating datasets and resources from all_data dictionary
                    dataJSON=[]
                    dataset_names = ['CanopyBaseHeight','CanopyBulkDensity','CanopyCover','CanopyHeight','CanopyLayerCount','LadderFuelDensity','SurfaceFuels']

                    for dataset in dataset_names:
                        resources=[]
                        for data in all_data:
                            if data['properties']['metric']==dataset:
                                resources.append({'name':data['id'],'title':data['id'],'description':data['description'],'url':data['assets'][dataset]['href'],'format':'tiff'})  
                    
                        temporal_extent = [x.encode('utf-8') for x in data['extent']['temporal']['interval'][0]] #convert from unicode to utf-8
                        payload = {
                                    'name':dataset.lower(), 
                                    'id':random.randint(10000000,1000000000), 
                                    'title':dataset,
                                    'notes' :data['description'],
                                    'tags':data['tags'],
                                    'license_id':data['license'],
                                    'url':"https://storage.googleapis.com/cfo-public/catalog.json",
                                    'extras': [{'key':'spatial','value':str(bbox_to_polygon(data['extent']['spatial']['bbox'][0]))},
                                        {'key':'temporal','value':str(json.dumps({"startTime": temporal_extent[0], "endTime": temporal_extent[1]}))},
                                        {'key':'providers','value':str(convert_unicode_to_str(data['providers']))}],
                                    'resources':resources  
                                    }
                        
                        dataJSON.append(payload)

            return dataJSON

        def get_opentopo_data(domain):
    
            catalog = requests.get(domain)
            catalog_result = catalog.json()
            
            # get all the datasets in stac format
            all_data=[]          
            for dataset in catalog_result['links']:
                if dataset['rel']=='child':
                    result_ = requests.get(dataset['href'])
                    output=result_.json()
                    all_data.append(output)
                    
            #convert to ckan format
            
            dataJSON=[]
            
            for data in all_data:
            
                #get resources
                resources=[]
                for item in data['links']:
                    if item['rel']=='child':
                        result_ = requests.get(item['href'])
                        out=result_.json()
                    
                        resources.append(
                                    {
                                    'name':out['assets']['Data']['title'],
                                    'title':out['assets']['Data']['title'],
                                    'description':out['description'],
                                    'url':out['assets']['Data']['href'],
                                    'format':'laz'
                                    }
                                )
                url=[element['href'] for element in data['links'] if element['rel'] == 'self'][0]     
                temporal_extent = [x.encode('utf-8') for x in data['extent']['temporal']['interval'][0]] #convert from unicode to utf-8

                payload = {
                            'name':data['title'].lower(), 
                            'id':random.randint(10000000,1000000000), 
                            'title':data['title'],
                            'notes' :data['description'],
                            'tags':data['keywords'],
                            'license_id':data['license'],
                            'url':url,
                            'extras': [{'key':'spatial','value':str(bbox_to_polygon(data['extent']['spatial']['bbox'][0]))},
                        {'key':'temporal','value':str(json.dumps({"startTime": temporal_extent[0], "endTime": temporal_extent[1]}))},
                        {'key':'sci:doi','value':data['sci:doi']},
                              {'key':'sci:citation','value':data['sci:citation']},
                              {'key':'providers','value':str(data['providers'])}],
                            'resources':resources
                            }
                        
                dataJSON.append(payload)

            return dataJSON
        
        
        
        
        def _make_harvest_objs(datasets):
            '''Create HarvestObject with STAC dataset content.'''
            obj_ids = []
            guids = []
            for d in datasets:
                log.debug('Creating HarvestObject for {} {}'
                          .format(d['title'],
                                  d['id']))
                obj = HarvestObject(guid=d['id'],
                                    job=harvest_job,
                                    content=json.dumps(d),
                                    extras=[HarvestObjectExtra(
                                                        key='status',
                                                        value='hi!')])
                obj.save()
                obj_ids.append(obj.id)
                guids.append(d['id'])
            return obj_ids, guids

        log.debug('In StacHarvester gather_stage (%s)',
                  harvest_job.source.url)

        #domain = urlparse(harvest_job.source.url).hostname
        domain = harvest_job.source.url
        
        
        if domain == 'https://storage.googleapis.com/cfo-public/vegetation/collection.json':
        
            object_ids, guids = _make_harvest_objs(get_cfo_data(domain))

        else:
            object_ids, guids = _make_harvest_objs(get_opentopo_data(domain))
        

        # Check if some datasets need to be deleted
        object_ids_to_delete = \
            self._mark_datasets_for_deletion(guids, harvest_job)

        object_ids.extend(object_ids_to_delete)

        return object_ids


    def fetch_stage(self, harvest_object):
        '''
        The fetch stage will receive a HarvestObject object and will be
        responsible for:
            - getting the contents of the remote object (e.g. for a CSW server,
            perform a GetRecordById request).
            - saving the content in the provided HarvestObject.
            - creating and storing any suitable HarvestObjectErrors that may
            occur.
            - returning True if everything is ok (ie the object should now be
            imported), "unchanged" if the object didn't need harvesting after
            all (ie no error, but don't continue to import stage) or False if
            there were errors.

        :param harvest_object: HarvestObject object
        :returns: True if successful, 'unchanged' if nothing to import after
                all, False if not successful
        '''
        # No fetch required, all package data obtained from gather stage.
        return True

    def import_stage(self, harvest_object):
        '''
        The import stage will receive a HarvestObject object and will be
        responsible for:
            - performing any necessary action with the fetched object (e.g.
            create, update or delete a CKAN package).
            Note: if this stage creates or updates a package, a reference
            to the package should be added to the HarvestObject.
            - setting the HarvestObject.package (if there is one)
            - setting the HarvestObject.current for this harvest:
            - True if successfully created/updated
            - False if successfully deleted
            - setting HarvestObject.current to False for previous harvest
            objects of this harvest source if the action was successful.
            - creating and storing any suitable HarvestObjectErrors that may
            occur.
            - creating the HarvestObject - Package relation (if necessary)
            - returning True if the action was done, "unchanged" if the object
            didn't need harvesting after all or False if there were errors.

        NB You can run this stage repeatedly using 'paster harvest import'.

        :param harvest_object: HarvestObject object
        :returns: True if the action was done, "unchanged" if the object didn't
                need harvesting after all or False if there were errors.
        '''
        log.debug('In StacHarvester import_stage')

        base_context = {
            'model': model,
            'session': model.Session,
            'user': self._get_user_name(),
            'ignore_auth': True
        }

        status = self._get_object_extra(harvest_object, 'status')
        if status == 'delete':
            # Delete package
            toolkit.get_action('package_delete')(
                base_context, {'id': harvest_object.package_id})
            log.info('Deleted package {0} with guid {1}'
                     .format(harvest_object.package_id, harvest_object.guid))
            return True

        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        # Get the last harvested object (if any)
        previous_object = model.Session.query(HarvestObject) \
            .filter(HarvestObject.guid == harvest_object.guid) \
            .filter(HarvestObject.current == True) \
            .first()  # noqa

        # Flag previous object as not current anymore
        if previous_object:
            previous_object.current = False
            previous_object.add()

        # Flag this object as the current one
        harvest_object.current = True
        harvest_object.add()

        # Check if a dataset with the same guid exists
        existing_dataset = self._get_existing_dataset(harvest_object.guid)

        # Delete package (dev testing)
        # if existing_dataset:
        #     self._delete_dataset(existing_dataset['id'])
        # return False

        package_dict = self._build_package_dict(base_context, harvest_object)

        self.process_package(package_dict, harvest_object)

        if existing_dataset:
            # Do we need to update?
            existing_updated_at = self._get_package_extra(existing_dataset,
                                                          'source_updated_at')
            source_updated_at = self._get_package_extra(package_dict,
                                                        'source_updated_at')
            if existing_updated_at and source_updated_at and \
               parse(existing_updated_at) == parse(source_updated_at):
                return 'unchanged'

            package_dict['id'] = existing_dataset['id']
            harvest_object.package_id = package_dict['id']
            harvest_object.add()
            try:
                toolkit.get_action('package_update')(
                    base_context.copy(),
                    package_dict
                )
            except Exception as e:
                self._save_object_error('Error updating package for {}: {}'
                                        .format(harvest_object.id, e),
                                        harvest_object, 'Import')
                return False

        else:
            # We need to explicitly provide a package ID
            package_dict['id'] = unicode(uuid.uuid4())

            harvest_object.package_id = package_dict['id']
            harvest_object.add()

            # Defer constraints and flush so the dataset can be indexed with
            # the harvest object id (on the after_show hook from the harvester
            # plugin)
            model.Session.execute(
                'SET CONSTRAINTS harvest_object_package_id_fkey DEFERRED')
            model.Session.flush()

            try:
                toolkit.get_action('package_create')(
                    base_context.copy(),
                    package_dict
                )
            except Exception as e:
                self._save_object_error('Error creating package for {}: {}'
                                        .format(harvest_object.id, e),
                                        harvest_object, 'Import')
                return False

        return True
