from pydicom.dataset import Dataset
from pydicom.tag import Tag
from pydicom.dataelem import DataElement
from pydicom.datadict import dictionary_VR

from pynetdicom import AE, evt, build_role, StoragePresentationContexts, PYNETDICOM_IMPLEMENTATION_UID, \
    PYNETDICOM_IMPLEMENTATION_VERSION


from pynetdicom.sop_class import PatientRootQueryRetrieveInformationModelMove, \
    PatientRootQueryRetrieveInformationModelFind, CTImageStorage, MRImageStorage, VerificationSOPClass

import traceback
import re
import os
import json

class PAC:

    # ==================================================================
    # CLASS VARIABLES
    # ==================================================================

    DESTINATION_DIRECTORY = './downloads/'
    MOVE_STUDYUIDS = {}
    STUDYUID_MAP = {}
    ANONYMIZE_HEADERS = True

    main = None

    # ==================================================================
    # CLASS CONSTANTS
    # ==================================================================

    # --- Standard Tags that we ask for in our initial SEARCH/FIND query

    MASTER_TAGNAME_TO_TAG = {}


    # --- ANONYMIZATION parameters
    # Defaults for use to use to anonymize our DICOM headers
    VR_DEFAULTS = {
        'SH': 'anonymous',  # Short String
        'IS': '',           # INTEGER String
        'LO': 'anonymous',  # Long String
        'DT': 'anonymous',  # DateTime
        'DA': 'anonymous',  # Date
        'TM': 'anonymous',  # Time
        'UI': 'anonymous',  # Unique Identifier, UID
        'PN': 'anonymous',  # Person Name
        'SQ': []            # Sequence
    }
    # blacklist of tags that, if present, means we drop the entire image
    ANON_IMGS = {}
    # VR (Value Representation) types that should be entirely wiped
    ANON_VRS = []
    # Specific Tags that should be wiped, not covered by the VR types listed above
    ANON_TAGS = {}
    # Individual Anon Tags not covered by the Anon VRs == (ANON_TAGS - ANON_VRS)
    ANON_TAGS_MINUS_ANON_VRS = {}


    """
    # --- UI table headers
    TAGS_TO_PRETTY = {
        # Our extra UI tags
        'status': 'Status',
        'matches': 'Matches',
        # Actual tags
        'PatientID': 'MRN',
        'accession': 'Accession',
        'StudyDate': 'Date',
        'Modality': 'Modality',
        'study_description': 'Description',
        'history': 'History',
        'setting': 'Setting',
        'location': 'Location',
        'age': 'Age',
        'referrer': 'Referrer',
        'body_part': 'Body Part',
        'studyUID': 'Study UID',
        'seriesUID': 'Series UID'
    }
    PRETTY_TO_TAGS = {v: k for k, v in TAGS_TO_PRETTY.items()}
    """

    # --- Default Query Template
    QUERY = None

    QUERY_TAGS_SORTED = None

    # ==================================================================
    # CLASS INIT
    # ==================================================================

    def __init__(self, master_tagname_to_tag, query_tags_sorted, anon_imgs, anon_vrs, anon_tags, main=None):
        self.main = main

        # --- Anonymization parameters
        # blacklist of tags that, if present, means we drop the entire image
        self.ANON_IMGS = anon_imgs
        # blacklist of VR types to completely wipe out
        self.ANON_VRS = anon_vrs
        # blacklist of specific tags to wipe out
        self.ANON_TAGS = anon_tags
        # Used to avoid redundant anonymization
        self.ANON_TAGS_MINUS_ANON_VRS = {vr: tags for vr, tags in self.ANON_TAGS.items() if vr not in self.ANON_VRS}

        # --- Master list of all tag info we are aware of
        self.MASTER_TAGNAME_TO_TAG = master_tagname_to_tag

        # --- Query template with tags of interest
        self.QUERY_TAGS_SORTED = query_tags_sorted
        # Create the default query template
        self.QUERY = {tag: [] for tag in self.QUERY_TAGS_SORTED}

    # ==================================================================
    # GETTERS/SETTERS
    # ==================================================================

    # tagname == 'PatientID'
    # tag == Tag([0x0010, 0x0020])
    def get_tag_from_tagname(self, key):
        return Tag(*self.MASTER_TAGNAME_TO_TAG[key][:2])

    def get_vr_of_tag(self, tag):
        return dictionary_VR(tag)

    # - ADDING NEW DICOM TAGS IS A PAIN IN THE BUTT - have to know the VR of the tag that you want
    def set_tag(self, ds, tag, value):
        # Set tag with value and insert into ds
        ds.add(DataElement(tag, self.get_vr_of_tag(tag), value))
        return ds

    # ==================================================================
    # METHODS
    # ==================================================================

    # remove non-alphanumeric characters
    def squish(self, s, word_limit=None, replace_with=''):
        p = '[^0-9a-zA-Z]+'
        s = str(s)
        if word_limit is not None:
            splt = re.split(p, s)
            print(splt)
            ss = replace_with.join(splt[:word_limit])
        else:
            ss = re.sub(p, replace_with, s) if s else ''

        return ss




    def strHex(self, i):
        return ('0x%04x' % i)


    # pretty print the dataset, organized by VR
    def print_ds_by_vr(self, ds):
        VR_tags = {}
        for tagname in ds.dir():
            # don't try to print the entire image by text...
            if tagname == 'PixelData':
                continue
            de = ds.data_element(tagname)
            vr = str(de.VR)
            tagraw = str(de.tag)
            val = str(de.value)
            tag = tagname + ' ' + str(tagraw).replace('(', '[').replace(')', ']')

            if vr not in VR_tags:
                VR_tags[vr] = {}
            VR_tags[vr][tag] = val

        print(json.dumps(VR_tags, indent=4))

    # Find all the tagnames/tags in this dataset and use them to form a mapping of tagname->tag
    def rip_TAGS(self, ds):
        tagname_to_tag = {}
        for tagname in ds.dir():
            tag = ds.data_element(tagname).tag
            tagname_to_tag[tagname] = [tag.group, tag.elem]

        return tagname_to_tag

    def print_all_TAGS(self, ds):
        tagname_to_tag = {}
        for tagname in ds.dir():
            de = ds.data_element(tagname)
            tag = de.tag
            hextag = '[%s, %s, \'%s\']' % (self.strHex(tag.group), self.strHex(tag.elem), de.VR)
            tagname_to_tag[tagname] = hextag

            print('\'%s\': %s,' % (tagname, tagname_to_tag[tagname]))

    def save_all_TAGS(self, ds):
        path = 'tags.json'
        tagname_to_tag = {}
        for tagname in ds.dir():
            de = ds.data_element(tagname)
            tag = de.tag

            group = self.strHex(tag.group)
            elem = self.strHex(tag.elem)
            vr = de.VR

            tagname_to_tag[tagname] = [group, elem, vr]

        with open(path, 'w') as outfile:
            json.dump(tagname_to_tag, outfile, sort_keys=True, indent=4)

    # filter out those naughty naughty VRs
    def vr_anon_callback(self, dataset, data_element):
        vr = data_element.VR
        if vr in self.ANON_VRS:
            data_element.value = self.VR_DEFAULTS[vr]

    # anonymize based on 'self.ANON_VRS' and 'self.ANON_TAGS'
    def anonymize_dataset(self, ds, anon_studyUID):
        # --- Print out the DS tags/values, organized by VR
        # self.print_ds_by_vr(ds)
        # print(ds)
        # self.save_all_TAGS(ds)
        # self.print_all_TAGS(ds)


        # ------------------ 0. If dataset has any of these tag values, just drop it completely
        for tagname in self.ANON_IMGS:
            if tagname in ds:
                ds_val = str(ds.data_element(tagname).value).lower()
                for val in self.ANON_IMGS[tagname]:
                    val = str(val).lower()
                    if val in ds_val:
                        #print('ANON: '+tagname + ' == ' + str(ds_val))
                        return None


        # ------------------ 1. Remove private tags for shirts and gurgles.
        # I still have no idea if this actually does anything
        ds.remove_private_tags()

        # ------------------ 2. Filter by VR type
        # uses 'self.ANON_VRS'
        ds.walk(self.vr_anon_callback)

        # ------------------ 3. Filter additional tags that weren't wiped by the VR filter
        # uses 'self.ANON_TAGS'
        for vr, tagnames in self.ANON_TAGS_MINUS_ANON_VRS.items():
            for tagname in tagnames:
                if tagname in ds:
                    ds.data_element(tagname).value = self.VR_DEFAULTS[vr]

        # ------------------ 3.5 AGE IS A SPECIAL CASE
        # age >89 is a hipaa violator, so if we have anonymization enabled but don't explicitly have PatientAge in
        # our list of tags to anonymize, we need to change any ages above 089Y to equal 089Y
        patientage = 'PatientAge'
        if patientage in ds:
            age_match = re.search('\d+', str(ds.data_element(patientage).value))
            if age_match is not None:
                age = int(age_match.group())
                if age > 89:
                    ds.data_element(patientage).value = '089Y'

        # ------------------ 4. Set the studyUID of the header
        # for navigational/sorting purposes while maintaining anonymity
        if anon_studyUID:
            ds.StudyInstanceUID = anon_studyUID

        # --- Print out the anonymized dicom header
        # self.print_ds_by_vr(ds)

        return ds

    # --- Find and set the appropriate level of specificity for the query. Does 3 things:
    # 1) determines the deepest level of detail (STUDY vs SERIES)
    # 2) sets the QR level property to this level
    # 3) removes queries associated with deeper, unused levels of detail

    def establish_qrlevel(self, ds, default_level='SERIES', levels=['PATIENT', 'STUDY', 'SERIES']):
        query_retrieve_level = default_level
        idx_cur_lvl = levels.index(default_level)

        # --- Check all levels that are DEEPER than our default level, to see if we need to dive deeper
        for idx_next_lvl in range(idx_cur_lvl+1, len(levels)):
            next_lvl = levels[idx_next_lvl]

            # --- Check all tags in the dataset that have the next level in their name. If at least one of those tags
            # is non-blank, then we dive deeper!
            # ds.dir() is CASE-INSENSITIVE
            for tag in ds.dir(next_lvl):
                if (tag in ds) and (ds.data_element(tag).value) and (str(ds.data_element(tag).value).lower() != 'anonymous'):
                    query_retrieve_level = next_lvl
                    break

        # --- Set the deepest level we found
        ds.QueryRetrieveLevel = query_retrieve_level

        # --- Remove tags associated with any deeper
        idx_cur_lvl = levels.index(query_retrieve_level)
        for idx_next_lvl in range(idx_cur_lvl+1, len(levels)):
            next_lvl = levels[idx_next_lvl]
            for tag in ds.dir(next_lvl):
                delattr(ds, tag)

        return ds


    def perform_find(self, self_info, peer_info, query={}, verbose=[], xlsx_file=None, results=None):

        # ------------------ 1. CONNECT TO PEER
        ae = AE()
        ae.ae_title = self_info['peer_aet']
        # --- Add a requested presentation contexts (contexts that our SCU will SEND aka REQUEST)
        ae.add_requested_context(PatientRootQueryRetrieveInformationModelFind)

        # --- holla
        assoc = ae.associate(peer_info['peer_ip'], int(peer_info['peer_port']), ae_title=peer_info['peer_aet'])

        # ------------------ 2. CONFIGURE QUERY
        # - Add any tags of interest that were passed into this function.
        # >Tags with associated values: act as filters - any response will match those parameters
        # >Tags without associated values: acts as a wildcard (will match anything) and thus act as queries - the response
        # will return the values for those tags

        # 'results' is a dict of tags/lists where we will store our query results in a (heading -> column) mapping
        if results is None:
            results = {}

        # --- Craft the final query dataset structure using our passed in query dict
        ds = Dataset()
        for tag_name, value in query.items():
            ds = self.set_tag(ds=ds, tag=self.get_tag_from_tagname(tag_name), value=value)

            # --- Add these tag names into the results dict as well
            if tag_name not in results:
                results[tag_name] = []

        # --- define QueryRetrieveLevel (PATIENT, STUDY, SERIES, IMAGE)
        # 'PATIENT' level == return one result per unique patient
        # 'STUDY' level == return one result per unique study
        # 'SERIES' level == return one result per unique series
        # use SERIES if we have it, otherwise default to STUDY
        ds = self.establish_qrlevel(ds)

        """
        # We always want at bare minium a studyUID, so here we guarantee that it is in the query
        if 'studyUID' not in query:
            ds = self.set_tag(ds=ds, tag=self.get_tag_from_tagname('studyUID'), value='*')
            query['studyUID'] = '*'
        if 'studyUID' not in results:
            results['studyUID'] = []
        """

        # ------------------ 3. PERFORM QUERY (C-FIND)
        # Query model is 'P' for Patient. This isn't something we need to worry about, it's just the standard way to
        # organize the data
        #print('----- FIND -----')
        responses_find = assoc.send_c_find(ds, query_model='P')

        # ------------------ 4. PROCESS QUERY RESULTS
        # C-FIND returns a list of tuples (status-code, dataset) that match the query. push the datasets into 'matches'
        matches = []
        # Parse through each raw query response, and save the identifiers
        for (status, identifier) in responses_find:
            if status:
                # If the find status is still 'Pending' then `identifier` describes one matching study
                if status.Status in (0xFF00, 0xFF01):
                    matches.append(identifier)
                else:
                    # Find is complete, no more matching studies
                    # if len(verbose) > 1: print('\t\tC-FIND Complete')
                    pass
            else:
                if len(verbose) > 0: print('\t\tConnection timed out, was aborted or received invalid response')

        # ------------------ 5. Prepare to return a dictionary of our find results
        # FOR EACH MATCH FOR THIS PARTICULAR QUERY
        for match in matches:
            output = ''
            # --- push the results of this match into the 'results' dict
            for tag in query:
                value = ''
                # Use the tag tuple identifier (defined in pac.TAGS) to check tags in the query
                # response
                val = match.get(self.get_tag_from_tagname(tag))
                if val is not None:
                    value = str(val.value)
                results[tag].append(value)

        # ------------------ 6. Release the connection to the peer
        assoc.release()

        # ------------------ 7. Spread the word of our lord and savior jesus christ
        return results

    # Handle the returning C-STORE response in response to our C-MOVE request
    # >THE DATA IS ALREADY DOWNLOADED AT THIS POINT, we're just deciding how to save it to file
    # >on_c_store is DEPRECATED, but it works and is more convenient than the new way so we're keeping it
    def handle_store(self, event):
        """
        Store the pydicom Dataset `ds` on our local file system
        --Parameters--
        ds (pydicom.dataset)  ==  Dataset The dataset that the peer has requested be stored.
        context (namedtuple)  ==  The presentation context that the dataset was sent under.
        info (dict)           ==  Information about the association and storage request.
        """
        try:
            ds = event.dataset
            ds.file_meta = event.file_meta

            # ------------------ 3. Track the original studyUID/seriesUID
            # The original studyUID is used as an internal representation for us to track
            # completion of the transfer of this study. Use it as a key in our status-tracking structure,
            # 'self.MOVED_STUDYUIDS'
            original_studyuid = ds.StudyInstanceUID
            original_seriesuid = ds.SeriesInstanceUID
            # Track how many images we've successfully received for each original studyUID
            key = (original_studyuid, original_seriesuid)
            if key not in self.MOVED_STUDYUIDS:
                # stored, passed (due to anonymization), misc messages (e.g. errors)
                self.MOVED_STUDYUIDS[key] = [0, 0, None]
            if original_studyuid not in self.MOVED_STUDYUIDS:
                self.MOVED_STUDYUIDS[original_studyuid] = [0, 0]
            [series_stored, series_passed, message] = self.MOVED_STUDYUIDS[key]
            [study_stored, study_passed] = self.MOVED_STUDYUIDS[original_studyuid]

            # --- cool tips: check if tag is in dataset via:
            # >>> 'PatientsName' in ds
            # >>> ds.data_element('PatientsName')
            """
            # ------------------ 1. Add the DICOM File Meta Information
            meta = Dataset()
            meta.MediaStorageSOPClassUID = ds.SOPClassUID
            meta.MediaStorageSOPInstanceUID = ds.SOPInstanceUID
            meta.ImplementationClassUID = PYNETDICOM_IMPLEMENTATION_UID
            meta.ImplementationVersionName = PYNETDICOM_IMPLEMENTATION_VERSION
            meta.TransferSyntaxUID = context.transfer_syntax
            # Add the file meta to the dataset
            ds.file_meta = meta
            # Set the transfer syntax attributes of the dataset
            ds.is_little_endian = context.transfer_syntax.is_little_endian
            ds.is_implicit_VR = context.transfer_syntax.is_implicit_VR
            """


            # ------------------ 2. Apply the studyUID -> study directory mapping
            # If we have a studyUID->studydirname mappings
            if self.STUDYUID_MAP and ds.StudyInstanceUID in self.STUDYUID_MAP:
                # --- studyUID is henceforth replaced by a new anonymous identifier
                studydirname = self.STUDYUID_MAP[ds.StudyInstanceUID]

            # If no studyUID->studydir mappings, something clearly went wrong. But the show must go on. Tally ho. No
            # anonymization here, no sir.
            else:
                # --- store it by the mrn, date, accession number.
                # ain't got no time to make pretty names here, that's main.py's job
                studydirname = '%s_%s_%s' % (ds.PatientID, ds.StudyDate, ds.AccessionNumber)


            # ------------------ 5. IMAGE NAME
            # Image numbering/naming is not affected by anonymization
            #filename = 'img%04d.dcm' % (study_stored+1+study_passed)
            filename = '%04d_ser%03dimg%03d_%s_%s.dcm' % \
                       (study_stored+1+study_passed,
                        int(ds.SeriesNumber),
                        int(ds.InstanceNumber),
                        self.squish(ds.StudyDescription, word_limit=3),
                        self.squish(ds.SeriesDescription))

            # ------------------ 4. ANONYMIZATION
            if self.ANONYMIZE_HEADERS:
                ds = self.anonymize_dataset(ds, anon_studyUID=studydirname)
                # ds is None means that the anonymize function wants to drop this image completely.
                # Return without storing to file
                if ds is None:
                    series_passed += 1
                    self.MOVED_STUDYUIDS[key] = [series_stored, series_passed, 'IGNORED']
                    study_passed += 1
                    self.MOVED_STUDYUIDS[original_studyuid] = [study_stored, study_passed]
                    return 0x0000

            # ------------------ 6. Create directories as necessary for us to store our dataset
            # Full study directory path
            full_studydir = os.path.join(self.DESTINATION_DIRECTORY, studydirname)
            # Full image path
            full_path = os.path.join(full_studydir, filename)
            # Check and create folders
            if not os.path.isdir(full_studydir):
                os.makedirs(full_studydir)
            # if we are at the very first image saved for this study, clear any previous folder contents
            elif study_stored == 0:
                for fname in os.listdir(full_studydir):
                    del_path = os.path.join(full_studydir, fname)
                    try:
                        # delete any files in the folder (will not touch subfolders)
                        if os.path.isfile(del_path):
                            os.unlink(del_path)
                    except Exception as err:
                        traceback.print_stack()
                        print(err)

            # ------------------ 7. Write like an egyptian
            ds.save_as(full_path, write_like_original=False)

            # ------------------ 8. Update the status of this study in our handy dandy status dict
            series_stored += 1
            self.MOVED_STUDYUIDS[key] = [series_stored, series_passed, message]
            study_stored += 1
            self.MOVED_STUDYUIDS[original_studyuid] = [study_stored, study_passed]

        except Exception as err:
            traceback.print_stack()
            self.MOVED_STUDYUIDS[key] = [series_stored, series_passed, ('FAILED STORAGE: %s' % str(err))]
            print(err)

        # Return a 'Success' status
        return 0x0000

    def perform_move(self,
                     self_info,
                     source,
                     destination,
                     query={},
                     self_dir=None,
                     anonymize_headers=True,
                     studyuid_map=None,
                     moved_studyuids={},
                     verbose=False):

        # ------------------ 1. Prepare options/parameters
        # set storage directory
        self.DESTINATION_DIRECTORY = self_dir if self_dir is not None else self.DESTINATION_DIRECTORY

        # set studyUID->studydir mappings
        self.STUDYUID_MAP = studyuid_map

        # anonymize
        self.ANONYMIZE_HEADERS = anonymize_headers

        # Track success of each query (will only be updated if we are moving to our
        self.MOVED_STUDYUIDS = moved_studyuids


        # ------------------ 2. Create our SCU (outgoing query sender)
        ae = AE()
        ae.ae_title = self_info['peer_aet']
        # Add a requested presentation contexts (contexts that our SCU will SEND aka REQUEST)
        ae.add_requested_context(PatientRootQueryRetrieveInformationModelMove)

        # ------------------ 3. If we're transferring to ourselves, our SCU will double as a nonblocking SCP
        # START NON-BLOCKING STORAGE SCP in separate thread to receive the MOVE
        if destination['peer_name'] == self.main.SELF:
            ae.supported_contexts = StoragePresentationContexts
            handlers = [(evt.EVT_C_STORE, self.handle_store)]
            scp = ae.start_server(('', int(destination['peer_port'])), block=False, evt_handlers=handlers)

        # ------------------ 4. Connect to peer
        assoc = ae.associate(source['peer_ip'], int(source['peer_port']), ae_title=source['peer_aet'])

        # ------------------ 5. Create query
        # --- Add tags to dataset
        ds = Dataset()
        for tag_name, value in query.items():
            if value:
                ds = self.set_tag(ds=ds, tag=self.get_tag_from_tagname(tag_name), value=value)


        # --- Query Retrieve Level
        ds = self.establish_qrlevel(ds)

        # ------------------ 6. Perform C_MOVE - move files to the destination (which must already be whitelisted in
        # the peer SCP) Only need to send the AET of the destination, as the IP/port of that AET should already be
        # known in the peer SCP's whitelist
        responses = assoc.send_c_move(ds, destination['peer_aet'], query_model='P')

        # --- check what transpired
        for (status, identifier) in responses:
            if status:
                # If the status is 'Pending', then the identifier is the C-MOVE response
                if status.Status in (0xFF00, 0xFF01):
                    if verbose: print('    C-MOVE PENDING')
                    pass
                else:
                    if verbose: print('    C-MOVE COMPLETE')
                    pass
            else:
                if verbose: print('        Connection timed out, was aborted or received invalid response')
                pass

        # ------------------ 7. Close our local SCP
        scp.shutdown()

        # ------------------ 8. Release our SCU's connection to the peer
        assoc.release()

        # ------------------ 9. clear the anonymization mappings
        # technically don't need to do this since it's overwritten by the next call, but it helps me sleep at night
        self.STUDYUID_MAP = {}


        # ------------------ 10. return a dict with the names and counts of the original studyUIDs we've successfully
        # saved to file
        # 'self.MOVED_STUDYUIDS' is populated in the function on_c_store, which is called when our peer sends us data
        # and hence will only run when we ourselves are the MOVE destination
        return self.MOVED_STUDYUIDS

