from ckanext.harvest.harvesters.base import HarvesterBase
from ckan.plugins.core import implements
#from ckan.plugins.core import SingletonPlugin, implements
from ckanext.harvest.interfaces import IHarvester
import pystac

class StacHarvester(HarvesterBase):
#class StacHarvester(SingletonPlugin):
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
        """
        def _request_datasets_from_socrata(domain, limit=100, offset=0):
            api_request_url = \
                '{0}?domains={1}&search_context={1}' \
                '&only=datasets&limit={2}&offset={3}' \
                .format(BASE_API_ENDPOINT, domain, limit, offset)
            log.debug('Requesting {}'.format(api_request_url))
            api_response = requests.get(api_request_url)

            try:
                api_json = api_response.json()
            except JSONDecodeError:
                self._save_gather_error(
                    'Gather error: Invalid response from {}'
                    .format(api_request_url),
                    harvest_job)
                return None

            if 'error' in api_json:
                self._save_gather_error('Gather error: {}'
                                        .format(api_json['error']),
                                        harvest_job)
                return None

            return api_json['results']

        def _page_datasets(domain, batch_number):
            '''Request datasets by page until an empty array is returned'''
            current_offset = 0
            while True:
                datasets = \
                    _request_datasets_from_socrata(domain, batch_number,
                                                   current_offset)
                if datasets is None or len(datasets) == 0:
                    raise StopIteration
                current_offset = current_offset + batch_number
                for dataset in datasets:
                    yield dataset
        """
        def _make_harvest_objs(datasets):
            '''Create HarvestObject with STAC dataset content.'''
            obj_ids = []
            guids = []
            for d in datasets:
                log.debug('Creating HarvestObject for {} {}'
                          .format(d['resource']['name'],
                                  d['resource']['id']))
                obj = HarvestObject(guid=d['resource']['id'],
                                    job=harvest_job,
                                    content=json.dumps(d),
                                    extras=[HarvestObjectExtra(
                                                        key='status',
                                                        value='hi!')])
                obj.save()
                obj_ids.append(obj.id)
                guids.append(d['resource']['id'])
            return obj_ids, guids

        log.debug('In StacHarvester gather_stage (%s)',
                  harvest_job.source.url)

        #domain = urlparse(harvest_job.source.url).hostname
        # set and read the catalog
        catalog_url = "https://storage.googleapis.com/cfo-public/catalog.json"
        catalog = pystac.Catalog.from_file(catalog_url)
        veg = catalog.get_child('vegetation')
        items = veg.get_all_items()

        all_data=[]
        for item in veg.get_all_items():
            d={'name':item.properties['metric'],'title':item.id,'date':item.get_datetime(),'spatial_extent':item.bbox,'url':item.assets[item.properties['metric']].get_absolute_href()}
            all_data.append(d)
        
        object_ids, guids = _make_harvest_objs(all_data)
        #object_ids, guids = _make_harvest_objs(_page_datasets(domain, 100))

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
