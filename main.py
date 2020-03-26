import myGUI
import pacs

import traceback
import os
import json

import numpy as np
# --- These following 3 imports are needed for PyInstaller to work properly with pandas, because they somehow get
# dropped from numpy in the conversion process. Jank.
try:
    import numpy.random.common
    import numpy.random.bounded_integers
    import numpy.random.entropy
except Exception as err:
    pass

import pandas as pd

import queue
import threading

# ==================================================================
# ==================================================================


class Main:

    # ==================================================================
    # CLASS VARIABLES
    # ==================================================================
    # --- Our own identifier gets special treatment (e.g. cannot delete it from the list of peers)
    SELF = '<SELF>'


    # --- Formatting functions
    # dicts containing functions for value formatting/mapping when we (1) initially load an excel file, (2) convert
    # dataframe values to tag values, and (3) convert tag values to dataframe values
    parse_to_tag_func = {}
    parse_to_val = {}



    # ==================================================================
    # HARDCODED DEFAULT CONFIGURATIONS
    # ==================================================================
    # (only used if config files are faulty))

    # Default config
    DEFAULT_CONFIG_PATHS = {
        "config_user_defaults": "./configs/user_defaults.json",
        "config_peers": "./configs/peers.json",
        "config_tags": "./configs/tags.json",
        "config_query_identifiers": "./configs/query_identifiers.json",
        "config_query_tags": "./configs/query.json",
        "config_synonyms": "./configs/synonyms.json",
        "config_anon_vrs": "./configs/anon_vrs.json",
        "config_anon_tags": "./configs/anon_tags.json",
        "config_anon_imgs": "./configs/anon_imgs.json",
        "config_tag_to_excel_map": "./configs/excel_headings.json"
    }

    DEFAULT_USER_DEFAULTS = {
        "default_download_dir": "./downloads/",
        "default_xlsx_dir": "./xlsx/",
        "default_src": "BMC PACS",
        "default_dest": SELF,
        "default_column_selection": "StudyDescription",
        "default_prefix_anon": "HIPAA_",
        "default_prefix_raw": "RAW_",
        "default_suffix_initial_queries": "_0_INITIAL_QUERIES_",
        "default_suffix_search_results": "_1_SEARCH_RESULTS",
        "default_suffix_success": "_3_SUCCESSFUL_DOWNLOADS",
        "default_suffix_ignored": "_4_IGNORED_FOR_HIPAA",
        "default_suffix_deselected": "_5_USER_EXCLUDED",
        "default_suffix_failed": "_6_FAILED_TRANSFERS",
        "default_suffix_missing": "_7_STUDIES_NOT_DOWNLOADED",
        "default_suffix_legend": "_2_MASTER_KEY_LEGEND",
        "default_legend_tags": ["PatientID",
                                "StudyDate",
                                "StudyTime",
                                "StudyDescription",
                                "SeriesDescription",
                                "AccessionNumber",
                                "Modality",
                                "StudyInstanceUID",
                                "SeriesInstanceUID"]
    }

    DEFAULT_PEERS = {
                SELF: {
                    'peer_name': SELF,
                    'peer_aet': 'BU_RESEARCH',
                    'peer_ip': '10.160.73.180',
                    'peer_port': 50001,
                },
                'BMC PACS': {
                    'peer_name': 'BMC PACS',
                    'peer_aet': 'GEPACSQR',
                    'peer_ip': '10.153.51.130',
                    'peer_port': 4100,
                },
                'Conquest': {
                    'peer_name': 'Conquest',
                    'peer_aet': 'CONQUESTSRV1',
                    'peer_ip': 'localhost',
                    'peer_port': 5678,
                },
                'JVS': {
                    'peer_name': 'JVS',
                    'peer_aet': 'TESTSERVER',
                    'peer_ip': 'localhost',
                    'peer_port': 5678,
                }
            }

    DEFAULT_QUERY_IDENTIFIERS = [
        'PatientID',
        'StudyDate',
        'AccessionNumber',
        'StudyDescription',
        'SeriesDescription',
        'Modality',
        'SeriesNumber',
        'StudyInstanceUID',
        'SeriesInstanceUID'
    ]

    DEFAULT_TAG_TO_EXCEL = {
        'Query': ['ID', 'Query', 'Query ID', 'QueryID', 'Index'],
        'PatientID': ["MRN"],
        'StudyDate': ["Date", "Study Date", "Date of Study"],
        'StudyTime': ["Time", "Study Time", "Time of Study"],
        'StudyDescription': ["Study Description", "Description"],
        'SeriesDescription': ["Series Description"],
        'Modality': ["Mod"],
        "SeriesNumber": ["Series Number"],
        'StudyInstanceUID': ["StudyUID", "Study UID"],
        'SeriesInstanceUID': ["SeriesUID", "Series UID"],
        'AccessionNumber': ['Accession', "Accession Number"]
    }

    DEFAULT_QUERY_TAGS_SORTED = [
        "PatientID",
        "StudyDate",
        "StudyTime",
        "AccessionNumber",
        "StudyDescription",
        "SeriesDescription",
        "Modality",
        "SeriesNumber",
        "StudyInstanceUID",
        "SeriesInstanceUID",
        "PatientAge",
        "ReferringPhysicianName"
    ]

    DEFAULT_ANON_VRS = {
        'DT': 'DateTime',
        'DA': 'Date',
        'TM': 'Time',
        'UI': 'Unique Identifier UID',
        'PN': 'Person Name',
        'SQ': 'Sequence of Items'
    }

    DEFAULT_ANON_TAGS = {
        # Anon Tags of VR types that we don't want to completely wipe
        'DS': [
            'PatientBodyMassIndex',
            'PatientSize',
            'PatientWeight',
        ],
        'SH': [
            'AccessionNumber',
            'ImplementationVersionName',
            'PerformedProcedureStepID',
            'StationName',
            'StudyID',
        ],
        'IS': [
            'AcquisitionNumber',
        ],
        'LO': [
            'FillerOrderNumberImagingServiceRequest',
            'InstitutionName',
            'IssuerOfPatientID',
            'PatientAddress',
            'PatientID',
            'RequestingService',
        ],
        # Anon Tags that should be covered by the ANON_VR filter
        'DA': [
            'AcquisitionDate',
            'ContentDate',
            'InstanceCreationDate',
            'PatientBirthDate',
            'SeriesDate',
            'StudyDate',
        ],
        'TM': [
            'AcquisitionTime',
            'ContentTime',
            'InstanceCreationTime',
            'SeriesTime',
            'StudyTime',
        ],
        'UI': [
            'FrameOfReferenceUID',
            'ImplementationClassUID',
            'SOPClassUID',
            'SOPInstanceUID',
            'SeriesInstanceUID',
            'StudyInstanceUID',
            'TransferSyntaxUID',
        ],
        'PN': [
            'PatientName',
            'ReferringPhysicianName',
        ],
        'SQ': [
            'ProcedureCodeSequence',
            'ReferedPatientSequence',
            'ReferencedStudySequence',
            'RequestAttributeSequence',
        ]
    }

    DEFAULT_ANON_IMGS = {
        "Modality": ['SR'],
        "SeriesDescription": ['Summary', 'Report', 'Outside', 'Text'],
        "SeriesNumber": ['999'],
    }

    DEFAULT_MASTER_TAGNAME_TO_TAGS = {
        'AccessionNumber': ['0x0008', '0x0050', 'SH'],
        'AcquisitionDate': ['0x0008', '0x0022', 'DA'],
        'AcquisitionNumber': ['0x0020', '0x0012', 'IS'],
        'AcquisitionTime': ['0x0008', '0x0032', 'TM'],
        'BitsAllocated': ['0x0028', '0x0100', 'US'],
        'BitsStored': ['0x0028', '0x0101', 'US'],
        'Columns': ['0x0028', '0x0011', 'US'],
        'ContentDate': ['0x0008', '0x0023', 'DA'],
        'ContentTime': ['0x0008', '0x0033', 'TM'],
        'ConvolutionKernel': ['0x0018', '0x1210', 'SH'],
        'DataCollectionDiameter': ['0x0018', '0x0090', 'DS'],
        'DeviceSerialNumber': ['0x0018', '0x1000', 'LO'],
        'DistanceSourceToDetector': ['0x0018', '0x1110', 'DS'],
        'DistanceSourceToPatient': ['0x0018', '0x1111', 'DS'],
        'ExposureTime': ['0x0018', '0x1150', 'IS'],
        'FillerOrderNumberImagingServiceRequest': ['0x0040', '0x2017', 'LO'],
        'FilterType': ['0x0018', '0x1160', 'SH'],
        'FocalSpots': ['0x0018', '0x1190', 'DS'],
        'FrameOfReferenceUID': ['0x0020', '0x0052', 'UI'],
        'GeneratorPower': ['0x0018', '0x1170', 'IS'],
        'HighBit': ['0x0028', '0x0102', 'US'],
        'ImageOrientationPatient': ['0x0020', '0x0037', 'DS'],
        'ImagePositionPatient': ['0x0020', '0x0032', 'DS'],
        'ImageType': ['0x0008', '0x0008', 'CS'],
        'ImplementationClassUID': ['0x0002', '0x0012', 'UI'],
        'ImplementationVersionName': ['0x0002', '0x0010', 'SH'],
        'InstanceCreationDate': ['0x0008', '0x0012', 'DA'],
        'InstanceCreationTime': ['0x0008', '0x0013', 'TM'],
        'InstanceNumber': ['0x0020', '0x0013', 'IS'],
        'InstitutionName': ['0x0008', '0x0080', 'LO'],
        'IssuerOfPatientID': ['0x0010', '0x0021', 'LO'],
        'KVP': ['0x0018', '0x0060', 'DS'],
        'Manufacturer': ['0x0008', '0x0070', 'LO'],
        'ManufacturerModelName': ['0x0008', '0x1090', 'LO'],
        'Modality': ['0x0008', '0x0060', 'CS'],
        'PatientAddress': ['0x0010', '0x1040', 'LO'],
        'PatientAge': ['0x0010', '0x1010', 'AS'],
        'PatientBirthDate': ['0x0010', '0x0030', 'DA'],
        'PatientBodyMassIndex': ['0x0010', '0x1022', 'DS'],
        'PatientID': ['0x0010', '0x0020', 'LO'],
        'PatientName': ['0x0010', '0x0010', 'PN'],
        'PatientPosition': ['0x0018', '0x5100', 'CS'],
        'PatientSex': ['0x0010', '0x0040', 'CS'],
        'PatientSize': ['0x0010', '0x1020', 'DS'],
        'PatientWeight': ['0x0010', '0x1030', 'DS'],
        'PerformedProcedureStepID': ['0x0040', '0x0253', 'SH'],
        'PhotometricInterpretation': ['0x0028', '0x0004', 'CS'],
        'PixelRepresentation': ['0x0028', '0x0103', 'US'],
        'PixelSpacing': ['0x0028', '0x0030', 'DS'],
        'PositionReferenceIndicator': ['0x0020', '0x1040', 'LO'],
        'ProcedureCodeSequence': ['0x0008', '0x1032', 'SQ'],
        'ProtocolName': ['0x0018', '0x1030', 'LO'],
        'ReferencedPatientSequence': ['0x0008', '0x1120', 'SQ'],
        'ReferencedStudySequence': ['0x0008', '0x1110', 'SQ'],
        'ReferringPhysicianName': ['0x0008', '0x0090', 'PN'],
        'RequestAttributesSequence': ['0x0040', '0x0275', 'SQ'],
        'RequestingService': ['0x0032', '0x1033', 'LO'],
        'RescaleIntercept': ['0x0028', '0x1052', 'DS'],
        'RescaleSlope': ['0x0028', '0x1053', 'DS'],
        'RotationDirection': ['0x0018', '0x1140', 'CS'],
        'Rows': ['0x0028', '0x0010', 'US'],
        'SOPClassUID': ['0x0008', '0x0016', 'UI'],
        'SOPInstanceUID': ['0x0008', '0x0018', 'UI'],
        'SamplesPerPixel': ['0x0028', '0x0002', 'US'],
        'ScanOptions': ['0x0018', '0x0022', 'CS'],
        'SecondaryCaptureDeviceManufacturer': ['0x0018', '0x1016', 'LO'],
        'SecondaryCaptureDeviceManufacturerModelName': ['0x0018', '0x1018', 'LO'],
        'SecondaryCaptureDeviceSoftwareVersions': ['0x0018', '0x1019', 'LO'],
        'SeriesDate': ['0x0008', '0x0021', 'DA'],
        'SeriesDescription': ['0x0008', '0x103e', 'LO'],
        'SeriesInstanceUID': ['0x0020', '0x000e', 'UI'],
        'SeriesNumber': ['0x0020', '0x0011', 'IS'],
        'SeriesTime': ['0x0008', '0x0031', 'TM'],
        'SliceThickness': ['0x0018', '0x0050', 'DS'],
        'SoftwareVersions': ['0x0018', '0x1020', 'LO'],
        'SpecificCharacterSet': ['0x0008', '0x0005', 'CS'],
        'StationName': ['0x0008', '0x1010', 'SH'],
        'StudyDate': ['0x0008', '0x0020', 'DA'],
        'StudyDescription': ['0x0008', '0x1030', 'LO'],
        'StudyID': ['0x0020', '0x0010', 'SH'],
        'StudyInstanceUID': ['0x0020', '0x000d', 'UI'],
        'StudyTime': ['0x0008', '0x0030', 'TM'],
        'TableHeight': ['0x0018', '0x1130', 'DS'],
        'TemporalPositionIndex': ['0x0020', '0x9128', 'UL'],
        'TransferSyntaxUID': ['0x0002', '0x0010', 'UI'],
        'WindowCenter': ['0x0028', '0x1050', 'DS'],
        'WindowWidth': ['0x0028', '0x1051', 'DS'],
        'XRayTubeCurrent': ['0x0018', '0x1151', 'IS']
    }

    # ==================================================================
    # CLASS INIT
    # ==================================================================
    def __init__(self):
        # --- (dataframe value) to (tag), when we prep values to send a query
        self.parse_to_tag_func = {
            'PatientID': self.parse_mrn,
            'StudyDescription': self.parse_to_tag_studydescription,
            'SeriesDescription': self.parse_to_tag_seriesdescription
        }
        self.parse_to_tag_by_VR_func = {
            'DA': self.parse_to_tag_date,
            'TM': self.parse_to_tag_time,
            #'DT': self.parse_to_tag_datetime
        }

        # --- (tag) to (dataframe value), to take query results and convert them back for display/storage purposes
        self.parse_to_val_func = {
            'PatientID': self.parse_mrn,
            'Query': self.parse_pad_num,
            'Study': self.parse_pad_num
        }
        self.parse_to_val_by_VR_func = {
            'DA': self.parse_to_val_date,
            'TM': self.parse_to_val_time,
            #'DT': self.parse_to_val_datetime
        }


    # ----------------------- FUNCTIONS FOR FORMAT CONVERSIONS (dataframe value <-> DICOM Tag value)
    # both parse structures use the DATAFRAME representations of the tag names / column headings

    # --- Query numbering formatting
    def parse_pad_num(self, q_num, args=None):
        return '%04d' % int(q_num)

    # --- MRN conversions (ensure inclusion of leading zeros)
    def parse_mrn(self, mrn, args=None):
        if not mrn or mrn == '' or mrn == '*' or pd.isna(mrn):
            return ''
        key = 'MRN_LENGTH'
        len = int(args[key]) if args and key in args else 7
        return ('%0' + str(len) + 'd') % int(mrn)

    # --- DATE CONVERSIONS (including DATE RANGES)
    def parse_date(self, da, format):
        if not da or da == '' or da == '*' or pd.isna(da):
            return ''

        try:
            return pd.to_datetime(da).strftime(format)

        except ValueError as err:
            try:
                split_date = str(da).split('-')
                d1 = pd.to_datetime('/'.join(split_date[:len(split_date) // 2]).strip()).strftime(format)
                d2 = pd.to_datetime('/'.join(split_date[len(split_date) // 2:]).strip()).strftime(format)
                if d1 > d2:
                    raise Exception('Could not parse DATE: \'%s\'' % da)
                return d1 + '-' + d2
            except (TypeError, ValueError) as err:
                traceback.print_stack()
                print(err)
                raise Exception('Could not parse DATE: \'%s\'' % da)
    def parse_to_tag_date(self, da, args=None):
        return self.parse_date(da, '%Y%m%d')
    def parse_to_val_date(self, da, args=None):
        return self.parse_date(da, '%Y/%m/%d')

    # --- TIME CONVERSIONS (NOT including time ranges, though these are technically supported by
    def parse_time(self, tm, format):
        if not tm or tm == '' or tm == '*' or pd.isna(tm):
            return ''

        # if it's an int (e.g. in HHMMSS format) then cast it to 6 digits (HHMMSS format) *this casting assumes that
        # the int is meant to be read into HHMMSS format but just somehow lost the leading zeroes. This will give
        # incorrect results if you try to pass in a different format (e.g. HHMM or MMSS)
        try:
            tm = '%06d' % int(tm)
        except:
            pass

        # first try naked time formats (which do not specify date)
        potential_time_formats = ['%H:%M:%S', '%H%M%S']
        for potential_time_format in potential_time_formats:
            try:
                return pd.datetime.strptime(tm, potential_time_format).time().strftime(format)
            except ValueError as err:
                pass
        # failing that, see if it's a datetime format
        return pd.to_datetime(tm).strftime(format)
    def parse_to_tag_time(self, tm, args=None):
        return self.parse_time(tm, '%H%M%S')
    def parse_to_val_time(self, tm, args=None):
        try:
            # if tm is represented by an int, it might be missing leading zeros. There should be at least 6
            if not pd.isna(tm) and len(str(tm)) < 6 and len(str(tm)) > 1:
                tm = '%06d' % int(tm)
        except:
            pass
        return self.parse_time(tm, '%H:%M:%S')

    # --- DATETIME CONVERSIONS (NOT IMPLEMENTED)
    def parse_datetime(self, dt, format):
        if not dt or dt == '' or dt == '*' or pd.isna(dt):
            return ''
        return dt # return pd.to_datetime(dt).strftime(format)
    def parse_to_tag_datetime(self, dt, args=None):
        return self.parse_time(dt, '')
    def parse_to_val_datetime(self, dt, args=None):
        return self.parse_time(dt, '')

    def description_to_queryidentifier(self, descr, exact_match):
        descr = str(descr)
        # If it's blank, then standardize the blank to '' and we're done
        if pd.isnull(descr) or (descr == '') or (descr == '*'):
            description = ''
        # If we want to add wildcards, do it.
        elif not exact_match:
            description = ('*%s*' % str(descr))
        # otherwise yeah
        else:
            description = descr

        return description
    def parse_to_tag_studydescription(self, descr, args=None):
        key = '_EXACT_MATCH_STUDYDESCRIPTION_'
        exact_match = True if args and key in args and args[key] else False
        return self.description_to_queryidentifier(descr, exact_match)
    def parse_to_tag_seriesdescription(self, descr, args=None):
        key = '_EXACT_MATCH_SERIESDESCRIPTION_'
        exact_match = True if args and key in args and args[key] else False
        return self.description_to_queryidentifier(descr, exact_match)


    # Saves dataframe to xlsx
    def save_to_xlsx(self, ui, full_path, df, file_descriptor='the', create_dirs=False, include_index=False):
        while True:
            try:
                if create_dirs:
                    dirname = os.path.dirname(full_path)
                    if not os.path.isdir(dirname):
                        os.makedirs(dirname)
                df.to_excel(full_path, index=include_index)
                break
            except PermissionError as err:
                retry = ui.popupYesNo(
                    'ERROR: Could not save %s file. File is open in another program.\n'
                    '%s\n\nRETRY?' % (file_descriptor, full_path))
                if retry == 'No':
                    break
            except FileNotFoundError as err:
                retry = ui.popupYesNo(
                    'ERROR: Could not save %s file. Directory path is invalid.\n'
                    '%s\n\nRETRY?' % (file_descriptor, full_path))
                if retry == 'No':
                    break


    # Saves dict to json
    def save_to_json(self, full_path, data_dict, overwrite=True, sort_keys=False):

        if not full_path:
            return

        dir_path = os.path.dirname(full_path)

        # --- If we don't want to overwrite but a file with the same name already exists, then rename the old file
        if not overwrite:
            if os.path.isfile(full_path):
                splt = os.path.splitext(full_path)
                path_no_ext = splt[0]
                ext = splt[1]
                rename_to = path_no_ext + '.old' + ext
                i = 1
                # append numbers if necessary
                while os.path.isfile(rename_to):
                    rename_to = '%s.old(%d)%s' % (path_no_ext, i, ext)
                    i += 1
                os.rename(full_path, rename_to)

        # --- Create new, file with default template
        # Create directories
        if dir_path and not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        # Write to file
        with open(full_path, 'w') as outfile:
            json.dump(data_dict, outfile, indent=4, sort_keys=sort_keys)


    def load_json(self, path, default=None, overwrite=False, key=None, sort_keys=False):
        # passing in 'key' as a parameter means that we want the return value to be not the dict
        # itself, but rather a value inside of the dict, 'dict[key]'

        # --- Load the json files
        try:
            with open(path, 'r') as jsfile:
                json_data = json.load(jsfile)

            # ---
            # If we have a key, that means we are returning dict[key] rather than the dict itself
            if key:
                json_data = json_data[key]

            # --- If we are returing a dict and we do have a default defined, then ensure that the loaded dict has at
            # least all top level keys that are in the default dict
            elif not key and default:
                # diff == values that are in the default but not in the loaded data dict
                diff = {k: v for k, v in default.items() if k not in json_data}
                if diff:
                    json_data = {**json_data, **diff}
                    self.save_to_json(full_path=path, data_dict=json_data, overwrite=overwrite, sort_keys=sort_keys)

        # --- If can't load the config file for any reason, then (1) use default values and (2) recreate the config file
        except Exception as err:
            # traceback.print_stack()
            # --- use the default
            json_data = default

            # --- write the default to a json file
            if default:
                # --- json files MUST be dicts, so if our default value is not a dict, stick it into one as a wrapper
                if key:
                    default = {key: default}
                self.save_to_json(full_path=path, data_dict=default, overwrite=overwrite, sort_keys=sort_keys)

        return json_data


    def print_dict(self, d):
        for k, v in d.items():
            print('\'%s\': %s,'%(k, str(v)))



    # ONLY uses tags from pac.QUERY to craft the query, does not use any extraneous columns from the passed in dataframe. So
    # if you want to ask about a specific tag, it needs to be specified in your pac.QUERY template
    def craft_queries(self, ui, df, query_tags, master_tagname_to_tag, query_identifier_col='Query', args=None):
        queries = []
        formatting_failures = {}

        # --- Dataframe, with each row acting as essentially a dict full of query tags and values
        for _, row in df.iterrows():

            # --------- Format query tags/values

            # --- Query identifier
            q_id = row[query_identifier_col]

            # --- Query tags
            # Parse each tag
            # map from a column heading -> tag, as necessary
            # format the tag values using 'parse_func' if available, otherwise just cast to string
            q = {}
            for tag in query_tags:
                # --- Insert tags into the query. Only insert tags exist in our predefined 'query_tags'
                val = str(row[tag]) if ((tag in row) and (not pd.isnull(row[tag]))) else ''
                vr = master_tagname_to_tag[tag][2] if tag in master_tagname_to_tag else None
                # --- convert dataframe value to DICOM-compliant value as necessary
                # if the formatting fails, we don't want the program to stop so just try to continue with the raw values
                try:
                    if vr in self.parse_to_tag_by_VR_func and tag not in self.parse_to_tag_func:
                        val = self.parse_to_tag_by_VR_func[vr](val, args)
                    elif tag in self.parse_to_tag_func:
                        val = self.parse_to_tag_func[tag](val, args)
                except Exception as err:
                    traceback.print_stack()
                    formatting_failures.add(tag)
                    pass

                q[tag] = val

            # --- Each entry is [query_identifier, query]
            queries.append([q_id, q])

        # --- Popup to tell users of any columns that failed formatting
        if formatting_failures:
            ui.popupError('ERROR: Encountered errors while formatting the following columns:\n%s' %
                          str(formatting_failures))

        return queries, formatting_failures


    def perform_find(self, pac, self_info, peer_info, query, query_identifier, all_results):

        # --- Prepare the return dict
        # Add keys for all query fields
        # Add additional keys for 'Status' and 'Query'
        #
        if not all_results:
            all_results = {'Status': [], 'Query': []}
            for tag in query:
                if tag not in all_results:
                    all_results[tag] = []

        # Perform the C-FIND
        # Will throw #RuntimeError if connection failed/rejected
        results = pac.perform_find(self_info=self_info, peer_info=peer_info, query=query, verbose=[])

        # Process the results
        num_results = 0
        for tag in results:
            if len(results[tag]) > num_results:
                num_results = len(results[tag])

        # --- Deal with queries that had 1 or more matches
        if num_results > 0:
            # --- record the DICOM headers of the matches
            for tag in results:
                all_results[tag] += results[tag]

                if len(results[tag]) > num_results:
                    num_results = len(results[tag])

            # note the which original query this match is associated with
            for i in range(1, num_results + 1):
                #match_i = '%d/%d' %(i, num_results)
                all_results['Query'].append(query_identifier)

            # Update the status column
            all_results['Status'] += ['READY']*num_results

        # --- Deal with queries that had 0 matches
        else:
            # We still want an entry in our results table if we didn't get any matches for a query - just use the original
            # query as one row
            for tag in query:
                all_results[tag].append(query[tag])

            # note the number of matches for this query
            all_results['Query'].append(query_identifier)
            # Update the status column
            all_results['Status'].append('MISSING')

        return all_results


    def display_selected_peer(self, ui, peer_info):
        ui.set_input(ui.main_window, '_NAME_PEER_CFG_', peer_info['peer_name'])
        ui.set_input(ui.main_window, '_AET_PEER_CFG_', peer_info['peer_aet'])
        ui.set_input(ui.main_window, '_IP_PEER_CFG_', peer_info['peer_ip'])
        ui.set_input(ui.main_window, '_PORT_PEER_CFG_', peer_info['peer_port'])


    def updatePeers(self, ui, peers, src_highlight=None, dest_highlight=None, peer_highlights=None):

        lst_peers = list(peers.keys()) or ['']

        # --- Update Main Tab
        ui.set_combo(ui.main_window, '_COMBO_SRC_MAIN_', lst_peers, select=src_highlight)
        ui.set_combo(ui.main_window, '_COMBO_DEST_MAIN_', lst_peers, select=dest_highlight)

        # --- Update Settings Tab
        sorted_peers = sorted(lst_peers, key=lambda s: s.lower())
        ui.set_listbox(ui.main_window, '_LST_PEERS_CFG_', sorted_peers, highlights=peer_highlights)


    def save_json(self, path, d):
        with open(path, 'w') as outfile:
            json.dump(d, outfile, indent=4)

    def format_df_general(self, df):
        # --- start as strings for standardization
        df = df.astype(str)
        # --- strip whitespace
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        # --- cast everything to int if possible, but don't include the blanks/nans
        df = df.replace(r'^\s*$', np.nan, regex=True)
        for col in df.columns:
            df.loc[pd.notnull(df[col]), col] = df.loc[pd.notnull(df[col]), col].apply(pd.to_numeric,
                                                                                      downcast='integer',
                                                                                      errors='ignore')
        # --- replace nan/nat with ''
        df = df.fillna('')
        # --- standardize to string again, to lock in the formatting
        df = df.astype(str)

        return df
    def format_df_columns(self, df, master_tagname_to_tag, parse_func={}, parse_by_vr_func={}):
        for heading in df.columns:
            try:
                vr = master_tagname_to_tag[heading][2] if heading in master_tagname_to_tag else None
                if vr in parse_by_vr_func and heading not in parse_func:
                    df.loc[:, heading] = \
                        df[heading].map(lambda x: parse_by_vr_func[vr](x, None))
                elif heading in parse_func:
                    df.loc[:, heading] = \
                        df[heading].map(lambda x: parse_func[heading](x, None))
            except ValueError as err:
                print('Issue formatting column: %s\n%s' % (heading, err))

        return df
    def format_df(self, ui, df, master_tagname_to_tag, parse_func={}, parse_by_vr_func={}):
        # --------- General formatting
        df = self.format_df_general(df)
        # --- format individual columns
        df = self.format_df_columns(df, master_tagname_to_tag, parse_func, parse_by_vr_func)
        return df


    def apply_selections(self, df, selections, ignore={}):
        if selections is None or df is None:
            return df

        for heading, [yes, no] in selections.items():
            if heading in ignore:
                continue
            if heading in df:
                all = set(yes) | set(no)
                new = set(df[heading].unique()) - all
                yes = set(yes) | new
                if new:
                    selections[heading] = [yes, no]
            if yes is not None:
                df = df[df[heading].isin(yes)]

        return df

    # DOESN'T ACTUALLY ESTABLISH THE NUMBERING, FOR WHATEVER REASON LOL
    def establish_numbering(self, df, ordering, numbering_col='Study', unique_col='StudyInstanceUID'):
        old_index = 'old_index'
        df[old_index] = df.index
        if numbering_col in df.columns:
            df = df.drop([numbering_col], 1)
        # --- select only 1 of each unique_col, but with the exception of also keep all rows that have unique_col==''
        df_index_by_study = df.loc[
            (~df[unique_col].duplicated()) |
            (df[unique_col] == ''),
            [old_index, unique_col]
        ]
        # --- set the indexing based on this
        df_index_by_study = df_index_by_study.reset_index(drop=True)
        df_index_by_study.index += 1
        df_index_by_study[numbering_col] = df_index_by_study.index
        # --- convert the ints into 4-digit integer strings
        df_index_by_study.loc[:, numbering_col] = \
            df_index_by_study.loc[:, numbering_col].apply(lambda x: self.parse_pad_num(x))
        # --- Apply our established indexes to the main results dataframe
        df = df.merge(df_index_by_study, on=[old_index, unique_col], how='outer')
        df = df.drop(columns=[old_index])
        # --- propagate the indexes forward for any studies that span multiple rows (e.g. have more than 1
        # unique series identifier listed for a given study)
        df = df.fillna(method='ffill')
        df = df[ordering]

        return df

    def apply_dual_selections(self, df, dual_selections, all_prefix, ignore_independent_vals=[],
                              reestablish_numbering=False, ordering=None, numbering_col='Study', \
                                                                                 unique_col='StudyInstanceUID'):

        # --- Global filters
        for independent_var in dual_selections:
            for independent_val in dual_selections[independent_var]:
                if independent_val in ignore_independent_vals:
                    continue
                if independent_val.startswith(all_prefix):
                    for dependent_var in dual_selections[independent_var][independent_val]:

                        selected = dual_selections[independent_var][independent_val][dependent_var]['yes']
                        # available = dual_selections[independent_var][independent_val][dependent_var]['no']

                        df = df[df[dependent_var].isin(selected)]



        # --- Individual dual filters, separated and filtered individually then combined at the end
        lst_sub_dfs = []
        for independent_var in dual_selections:
            for independent_val in df[independent_var].unique():
                # --- Choose which subset of the dataframe that we will be applying our filter to
                sub_df = df[df[independent_var] == independent_val]

                # --- ignore any filters for this independent value's sub_df
                if independent_val in ignore_independent_vals:
                    lst_sub_dfs.append(sub_df)
                    continue

                # --- already dealt with the "all" case
                if independent_val.startswith(all_prefix):
                    continue



                # --- Apply the filter, if we have one
                if independent_val in dual_selections[independent_var]:

                    for dependent_var in dual_selections[independent_var][independent_val]:
                        selected = dual_selections[independent_var][independent_val][dependent_var]['yes']
                        sub_df = sub_df.loc[(sub_df[independent_var] == independent_val) &
                                            (sub_df[dependent_var].isin(selected))]
                # --- track our filtered sub_df
                lst_sub_dfs.append(sub_df)

        # combine all sub_dfs
        if lst_sub_dfs:
            df = pd.concat(lst_sub_dfs)

        # reset the numbering (basically always used to establish the "study" numbering)
        if reestablish_numbering:
            df = self.establish_numbering(df, ordering, numbering_col, unique_col)

        return df

    def generate_selections(self, df):

        # --- generate list of unique values for each column in the 'headings' list
        selections = {}
        headings = df.columns.values.tolist()
        for heading in headings:
            if heading in df:
                yes = df[heading].unique()
                no = []
                selections[heading] = [yes, no]

        return selections


    def load_excel(self, ui, path, required_headings=[], excel_to_tag={}):
        sheets = pd.read_excel(path, sheet_name=None)

        # --- Append each study of interest into a list of dicts, then display all valid entries
        # the DataFrame structure is the pandas library representation of a sheet in excel
        valid_sheets = {}
        # For now, ONLY GRAB THE FIRST SHEET
        for sheet_name in [list(sheets.keys())[0]]:
            try:
                df = sheets[sheet_name]
                # --- Try to standardize the excel column headers
                df = df.rename(columns=excel_to_tag)

                # --- Fill in missing tags and null values with np.nan as necessary
                # we honestly don't care what the user puts in their excel sheet. We have a predefined set of
                # tags (query_table_headings) that we theoretically want to use as identifiers in our query,
                # and if the excel sheet has any of those as columns, we'll use them. We drop/ignore any other columns.
                # ensure that all of our valid query fields are present
                df = df.replace('', np.nan)
                for heading in required_headings:
                    if heading not in df.columns:
                        df[heading] = np.nan

                # --- Drop rows where ALL of the values in the columns of interest are Na
                df = df.dropna(subset=required_headings, how='all')
                # --- standardize nans to the empty string ''
                df = df.fillna('')

                # --- Reset the index to not include those dropped rows
                df = df.reset_index(drop=True)

                # --- Add this processed sheet into our dict of valid sheets, with the relevant headers in the
                # relevant order
                # note that this DROPS ANY COLUMNS THAT ARE NOT IN query_table_headings
                valid_sheets[sheet_name] = df[required_headings if required_headings else df.columns]

            # If there's any problem reading the sheet we don't really care what it is, we just skip that sheet
            except Exception as err:
                traceback.print_stack()
                ui.popupError('ERROR: Skipping sheet [%s] in [%s]: \n%s' % (sheet_name, path, err))
                return None

        if not valid_sheets:
            ui.popupError('ERROR: NO VALID SHEETS FOUND IN XLSX FILE:  %s' % path)
            return None

        # --- FINALLY ACTUALLY LOAD THE PROCESSED ENTRIES INTO OUR SYSTEM (df_queries)
        # this line theoretically combines all the valid sheets together before into one big 'ol dataframe,
        # but since for now we're limiting it to reading just the first sheet in a file, the concat doesn't really
        # do anything. len(valid_sheets) will always be <= 1
        df_queries_temp = pd.concat([valid_sheets[s] for s in valid_sheets.keys()]) if len(valid_sheets) > 1 \
            else valid_sheets[list(valid_sheets.keys())[0]]

        return df_queries_temp


    def create_template_xlsx(self, columns, path):
        if not columns:
            return

        df_template = pd.DataFrame(columns=columns)

        # Create directories
        dir = os.path.dirname(path)
        if dir and not os.path.isdir(dir):
            os.makedirs(dir)

        # Create template file
        with pd.ExcelWriter(path) as writer:
            df_template.to_excel(writer, 'Sheet1', index=False)
            writer.save()


    def updateTable(self, ui, window, key, tbl):
        ui.set_table(window, key, tbl)


    def threaded_perform_moves(self,
                               thread_stop,
                               pac,
                               ui_queue,
                               df_results,
                               queries,
                               self_info,
                               peer_info,
                               dest_info,
                               anonymize,
                               skip_onfile,
                               self_move_dir,
                               studyuid_map):

        num_queries = len(queries)
        num_moves = 0

        try:
            # 7. --- PERFORM C-MOVE FOR EACH QUERY, IN ANOTHER THREAD
            # --- Before we start downloading, get our storage directory in order.
            # If we are overwriting instead of skipping any relevant study directories that are already on
            # local storage, we're essentially going to clear any

            # --- Work through the queries
            moved_studyuids = {}
            for i in range(num_queries):
                # query!
                [_, query] = queries[i]
                cur_studyUID = str(query['StudyInstanceUID'])
                cur_seriesUID = str(query['SeriesInstanceUID'])


                # --- update progressbar via queue
                key = '_T_PROGRESS_'
                msg = 'SOURCE:        %s\n' \
                      'DESTINATION:   %s\n' \
                      '\nTransferring %d out of %d...\n' \
                      '\nStudy #: %s\nStudy: %s\nSeries: %s' % \
                      (peer_info['peer_name'],
                       dest_info['peer_name'],
                       i + 1,
                       num_queries,
                       studyuid_map[cur_studyUID],
                       query['StudyDescription'],
                       query['SeriesDescription'])
                val = {
                    'title': 'Transferring Files (C-MOVE)',
                    'i': i,
                    'max_value': num_queries,
                    'key': '_T_PROGRESS_MOVE_',
                    'msg': msg,
                    'num_moves': num_moves
                }
                ui_queue.put([key, val])

                # 7a. --- skip C-MOVE for entries that we already have on file
                # CRITERIA: skip this entry if the STUDYDIRNAME folder exists*
                # *we don't check files inside the folder, because we don't have a way to check how many
                # files are actually supposed to be in a given study other than directly downloading it all

                if skip_onfile:
                    studydirpath = os.path.join(self_move_dir, studyuid_map[cur_studyUID])
                    if os.path.isdir(studydirpath):
                        # This monstrosity of a python statement counts how many *.dcm files are in the folder
                        num_dcm = len([f for f in os.listdir(studydirpath)
                                       if f.endswith('.dcm') and os.path.isfile(os.path.join(studydirpath, f))])

                        if num_dcm > 0:
                            plural = '' if num_dcm <= 1 else 's'
                            status = ('STUDY ON FILE: %d total image%s' % (num_dcm, plural))
                            df_results.loc[(df_results['StudyInstanceUID'] == cur_studyUID) &
                                           (df_results['SeriesInstanceUID'] == cur_seriesUID),
                                           'Status'] = status
                            continue

                # 7a. --- perform the C-MOVE
                moved_studyuids = pac.perform_move(self_info=self_info,
                                                   source=peer_info,
                                                   destination=dest_info,
                                                   query=query,
                                                   self_dir=self_move_dir,
                                                   anonymize_headers=anonymize,
                                                   studyuid_map=studyuid_map,
                                                   moved_studyuids=moved_studyuids,
                                                   verbose=False)

                # Count the number of transfers attempted
                num_moves += 1


                # ------------ UPDATE C-MOVE RESULT SPECIAL CASES

                # --- if we did not receive any data
                if key not in moved_studyuids:

                    # --- If MOVE destination is NOT ourself
                    if self_move_dir is None:
                        moved_studyuids[key] = [0, 0, 'TRANSFER REQUEST SENT: %s -> %s' % (peer_info['peer_name'],
                                                                                       dest_info['peer_name'])]
                    # --- If MOVE destination IS ourself
                    else:
                        # MOVE destination is ourself BUT the requested data NOT reach us
                        moved_studyuids[key] = [0, 0, 'FAILED TRANSFER']



                    # --- check the overall study tracker as well
                    if cur_studyUID not in moved_studyuids:
                        moved_studyuids[cur_studyUID] = [0, 0]



                # if we gotta stop, then stop.
                if thread_stop():
                    break

        except RuntimeError as err:
            traceback.print_stack()
            msg = '***CONNECTION ERROR***\nFailed to connect to peer: \n\nPeer Name: %s\nPeer AET:  %s\nPeer IP:   %s\n' \
                  'Peer Port: %s\n\nSelf Name: %s\nSelf AET:  %s\nSelf Port: %s' % \
                  (peer_info['peer_name'],
                   peer_info['peer_aet'],
                   peer_info['peer_ip'],
                   peer_info['peer_port'],
                   self_info['peer_name'],
                   self_info['peer_aet'],
                   self_info['peer_port'])
            key = '_T_ERROR_'
            val = {'msg': msg}
            ui_queue.put([key, val])

        # ------------ MOVES are done! update 'Status' column in df_results

        for entry in moved_studyuids:
            if isinstance(entry, str):
                continue
            else:
                [series_stored, series_passed, message] = moved_studyuids[entry]
                (cur_studyuid, cur_seriesuid) = entry
                [study_stored, study_passed] = moved_studyuids[cur_studyuid]
                prefix = 'DOWNLOADED:' if message is None else message + ': '
                counts = '%d/%d stored, %d/%d ignored' % (series_stored, study_stored, series_passed, study_passed)
                status = prefix + counts

                df_results.loc[
                    (df_results['StudyInstanceUID'] == cur_studyuid) &
                    (df_results['SeriesInstanceUID'] == cur_seriesuid),
                    'Status'] = status

        # --- CLOSE the progress bar via queue (if it was not stopped manually)
        if not thread_stop():
            key = '_T_PROGRESS_'
            val = {
                'title': 'Transferring Files (C-MOVE)',
                'i': num_queries,
                'max_value': num_queries,
                'key': '_T_PROGRESS_MOVE_',
                'msg': ''
            }
            ui_queue.put([key, val])

        # --- C-MOVES DONE
        key = '_T_MOVES_DONE_'
        val = {'moved_studyuids': moved_studyuids,
               'num_moves': num_moves,
               'df_results': df_results
               }
        ui_queue.put([key, val])



    def threaded_perform_finds(self,
                               thread_stop,
                               pac,
                               ui_queue,
                               queries,
                               self_info,
                               peer_info):

        # --- PERFORM A C-FIND FOR EACH QUERY
        # 'all_query_results' is a dict with the tag as the key and the list of results as the value. Which is
        # weird, but it facilitates conversion to a dataframe a few lines down
        try:
            # Perform each query
            all_query_results = {}
            num_queries = len(queries)
            for i in range(num_queries):
                [query_identifier, query] = queries[i]

                # --- update progressbar via queue
                key = '_T_PROGRESS_'
                msg = 'DATABASE:  %s\n' \
                      '\nSearching for query %d out of %d...\n' % \
                      (peer_info['peer_name'],
                       i + 1,
                       num_queries)
                val = {
                    'title': 'Searching Files (C-FIND)',
                    'i': i,
                    'max_value': num_queries,
                    'key': '_T_PROGRESS_FIND_',
                    'msg': msg
                }
                ui_queue.put([key, val])

                # --- Perform each C-FIND
                # default will check at SERIES level, so that we can decide which series we actually want to
                # download
                all_query_results = self.perform_find(pac, self_info, peer_info, query,
                                                      query_identifier, all_query_results)

                # if we gotta stop, then stop.
                if thread_stop():
                    break

        except RuntimeError as err:
            traceback.print_stack()
            msg = '***CONNECTION ERROR***\nFailed to connect to peer: \n\nPeer Name: %s\nPeer AET:  %s\nPeer IP:   %s\n' \
                  'Peer Port: %s\n\nSelf Name: %s\nSelf AET:  %s\nSelf Port: %s' % \
                  (peer_info['peer_name'],
                   peer_info['peer_aet'],
                   peer_info['peer_ip'],
                   peer_info['peer_port'],
                   self_info['peer_name'],
                   self_info['peer_aet'],
                   self_info['peer_port'])
            key = '_T_ERROR_'
            val = {'msg': msg}
            ui_queue.put([key, val])
            all_query_results = None

        # --- CLOSE the progress bar via queue
        if not thread_stop():
            key = '_T_PROGRESS_'
            val = {
                'title': 'Searching Files (C-FIND)',
                'i': num_queries,
                'max_value': num_queries,
                'key': '_T_PROGRESS_FIND_',
                'msg': ''
            }
            ui_queue.put([key, val])


        # --- C-FINDS DONE
        key = '_T_FINDS_DONE_'
        val = {'all_query_results': all_query_results}
        ui_queue.put([key, val])


    # *******************************
    # MAIN LOOP
    # *******************************

    def run(self):

        ALL_PREFIX = '***ALL'

        # *******************************
        # Threading
        # *******************************
        # Queue is THREAD-SAFE, is used to communicate between main ui loop and long-running network queries
        ui_queue = queue.Queue()
        thread_id = None



        # *******************************
        # LOAD CONFIG FILES
        # *******************************

        # --------- Load master config file - initial parameters and other config file locations
        config_fname = 'config.json'
        config = self.load_json(config_fname, default=self.DEFAULT_CONFIG_PATHS)

        # --------- Load Peers
        peers_key = 'config_peers'
        peers = self.load_json(config[peers_key], default=self.DEFAULT_PEERS)

        # --------- Load Master Dict of Known Tags
        tags_key = 'config_tags'
        master_tagname_to_tag = self.load_json(config[tags_key],
                                               default=self.DEFAULT_MASTER_TAGNAME_TO_TAGS,
                                               sort_keys=True)
        # convert 'tag group' and 'tag element' hex strings into their corresponding integer values
        master_tagname_to_tag = {k: [int(grp, 0), int(elm, 0), vr]
                                 for k, [grp, elm, vr]
                                 in master_tagname_to_tag.items()}

        # --------- Load Study Identifier Tags
        # only use tagnames that do exist in the master list
        identifier_key = 'config_query_identifiers'
        identifier_wrapper_key = 'QUERY_IDENTIFIERS'
        query_identifiers = self.load_json(config[identifier_key],
                                           default=self.DEFAULT_QUERY_IDENTIFIERS,
                                           key=identifier_wrapper_key)
        # tagnames are valid only if they do exist in the master list
        query_identifiers = [x for x in query_identifiers if x in master_tagname_to_tag]

        # --------- Load Query Template
        query_key = 'config_query_tags'
        query_wrapper_key = 'QUERY_TAGS'
        query_tags_sorted = self.load_json(config[query_key],
                                           default=self.DEFAULT_QUERY_TAGS_SORTED,
                                           key=query_wrapper_key)
        # include identifier tags in the query as well
        query_tags_sorted = query_tags_sorted + [x for x in query_identifiers if x not in query_tags_sorted]
        # tagnames are valid only if they do exist in the master list
        query_tags_sorted = [x for x in query_tags_sorted if x in master_tagname_to_tag]


        # --------- Load Anonymization Parameters
        # blacklist of tag values that, if present, means we drop the entire image. Case IN-sensitive
        anon_imgs_key = 'config_anon_imgs'
        anon_imgs = self.load_json(config[anon_imgs_key], default=self.DEFAULT_ANON_IMGS)
        # blacklist of VR types
        anon_vrs_key = 'config_anon_vrs'
        anon_vrs = self.load_json(config[anon_vrs_key], default=self.DEFAULT_ANON_VRS)
        # blacklist of individual header tags (stored in a dict under their respective VRs)
        anon_tags_key = 'config_anon_tags'
        anon_tags = self.load_json(config[anon_tags_key], default=self.DEFAULT_ANON_TAGS)

        # --------- Load the Tag-to-Excel map
        tag_to_excel_key = 'config_tag_to_excel_map'
        tag_to_excel = self.load_json(config[tag_to_excel_key], default=self.DEFAULT_TAG_TO_EXCEL, sort_keys=True)
        # Build our actual goal datastructure, the Excel-to-Tag map
        excel_to_tag = {}
        for k, v in tag_to_excel.items():
            for heading in v:
                excel_to_tag[heading] = k
                excel_to_tag[heading.lower()] = k
                excel_to_tag[heading.upper()] = k
                excel_to_tag[heading.capitalize()] = k

        # --------- Load Miscellaneous User Parameters
        user_defaults_key = 'config_user_defaults'
        user_defaults = self.load_json(config[user_defaults_key], default=self.DEFAULT_USER_DEFAULTS)

        anon_prefix = user_defaults['default_prefix_anon']
        raw_prefix = user_defaults['default_prefix_raw']
        initial_queries_suffix = user_defaults['default_suffix_initial_queries']
        search_results_suffix = user_defaults['default_suffix_search_results']
        success_suffix = user_defaults['default_suffix_success']
        ignored_suffix = user_defaults['default_suffix_ignored']
        deselected_suffix = user_defaults['default_suffix_deselected']
        missing_suffix = user_defaults['default_suffix_missing']
        failed_suffix = user_defaults['default_suffix_failed']
        legend_suffix = user_defaults['default_suffix_legend']
        legend_tags = user_defaults['default_legend_tags']
        legend_tags = [x for x in legend_tags if x in master_tagname_to_tag]
        src_highlight = user_defaults['default_src']
        dest_highlight = user_defaults['default_dest']
        xlsx_dir = user_defaults['default_xlsx_dir']
        download_dir = user_defaults['default_download_dir']
        default_column_selection = user_defaults['default_column_selection']


        # --- Create DOWNLOAD DIRECTORY and INPUT EXCEL FILE DIRECTORY if they do not already exist. Not terribly
        # important that these succeed, since the user is able to change where the program looks for these things
        # directly through the UI.
        try:
            if not os.path.isdir(xlsx_dir):
                os.makedirs(xlsx_dir)
        except FileNotFoundError as err:
            pass
        try:
            if not os.path.isdir(download_dir):
                os.makedirs(download_dir)
        except FileNotFoundError as err:
            pass

        # --- Create a TEMPLATE excel file if one does not already exist. Created as a courtesy for the user.
        # Draws information from:
        #       (1) 'user_config[template_path_key]' for the path
        #       (2) 'query_identifiers' for the excel column headers
        template_path = os.path.join(xlsx_dir, '_TEMPLATE_.xlsx')
        if not os.path.isfile(template_path):
            self.create_template_xlsx(query_identifiers, template_path)



        # *******************************
        # UI (User Interface)
        # *******************************

        # --- Table headings for the table displaying User Queries
        query_table_headings = ['Query'] + query_identifiers
        query_table_padding = [' ' * 6] * len(query_table_headings)

        # --- Table headings for the table displaying Query Results
        # 'Status' in particular requires a lot of space for text
        results_table_headings = ['Study', 'Status', 'Query'] + query_tags_sorted
        results_table_padding = [''] + \
                                [' ' * 23] + \
                                ([' ' * 6] * (len(results_table_headings) - 2))

        # --- START THE UI
        ui = myGUI.GUI()
        ui.createUI(query_table_headings,
                    results_table_headings,
                    padding_pretty_raw_main=query_table_padding,
                    padding_pretty_results_main=results_table_padding,
                    default_filter_main=default_column_selection)

        # DataFrame containing raw entries loaded from an input file
        df_queries = None
        # DataFrame containing results from our C-FIND/C-MOVE queries
        df_results = None
        # selection filters for which studies/series to perform the C-MOVE on
        selections = None
        # Lists of columns to sort the results table on
        sort_yes = query_table_headings
        sort_no = [x for x in results_table_headings if x not in query_table_headings]

        # Default path displayed for loading xlsx files
        load_path = os.path.abspath(xlsx_dir)

        # Default path displayed for local storage directory
        storage_path = os.path.abspath(download_dir)

        # --- Peers
        self.updatePeers(ui, peers, src_highlight=src_highlight, dest_highlight=dest_highlight)
        self.display_selected_peer(ui, peers[self.SELF])

        # --- Local storage directory
        ui.set_txt(ui.main_window, '_DIR_LOCAL_CFG_', storage_path)

        # --- Phase (0-3: LOAD, FIND, MOVE, DONE)
        # There is a necessary chain of steps to go through to successfully transfer an image. If the chain is broken or
        # done out of order, it may need to reset
        ui.set_phase(ui.PHASE_LOAD)




        # *******************************
        # PAC (netcode, DICOM stuff)
        # *******************************
        pac = pacs.PAC(master_tagname_to_tag, query_tags_sorted, anon_imgs, anon_vrs, anon_tags, main=self)




        # *******************************
        # MAIN UI EVENT LOOP
        # *******************************
        while True:
            event, values = ui.main_window.Read(timeout=100)
            #print(event, values)

            # ---------------
            # CLOSE PROGRAM
            # ---------------
            if event is None or event == 'Exit':
                break

            # ---------------
            # FEEDBACK/PROGRESS UPDATES FROM QUERY THREADS
            # ---------------
            try:
                [key, val] = ui_queue.get_nowait()
            except queue.Empty:
                key = None
                val = None

            if key is not None:
                if key == '_T_ERROR_':
                    ui.popup(val['msg'])
                    continue
                elif key == '_T_PROGRESS_':

                    # 7e. --- Update the progress bar.
                    continue_transfer = ui.OneLineProgressMeter(title=val['title'],
                                                                i=val['i'],
                                                                max_value=val['max_value'],
                                                                key=val['key'],
                                                                msg=val['msg'])
                    # If user closed the progress bar or pressed cancel, stop our transfer altogether
                    if not continue_transfer:
                        thread_stop = True
                        continue

                elif key == '_T_FINDS_DONE_':
                    all_query_results = val['all_query_results']

                    # confirm query results
                    if not all_query_results:
                        ui.set_phase(ui.PHASE_FIND)
                        continue

                    # ------------ Convert to dataframe and standardize
                    df_results_temp = pd.DataFrame(all_query_results)

                    # --- formatting (generalized column formatting + individual column formatting)
                    try:
                        df_results_temp = self.format_df(ui, df_results_temp, master_tagname_to_tag,
                                                         self.parse_to_val_func,
                                                         self.parse_to_val_by_VR_func)
                    except Exception as err:
                        traceback.print_stack()
                        ui.popupError('***ERROR***\nFailed to parse C-FIND QUERIES:\n%s - %s' %
                                      (type(err).__name__, err))
                        ui.set_phase(ui.PHASE_FIND)
                        continue

                    # --- Lock in df_results
                    df_results = df_results_temp

                    # ------- ESTABLISH ANONYMIZATION INDEXES BY PER-STUDY BASIS
                    # THIS IS THE ONLY TIME THAT WE RESET THE INDEX, so the index and resultant anonymization mapping
                    # should stay consistent. The indexes are established after sorting, by the preestablished
                    # "sorted query" column order
                    df_results = df_results.sort_values(by=query_table_headings)
                    df_results = self.establish_numbering(df_results,
                                                          ordering=results_table_headings,
                                                          numbering_col='Study',
                                                          unique_col='StudyInstanceUID')
                    # --- Assert the proper column order
                    df_results = df_results[results_table_headings]


                    # --- sort
                    if sort_yes:
                        df_results = df_results.sort_values(by=sort_yes)

                    # --- generate default study/series filters
                    selections = self.generate_selections(df_results)
                    dual_selections = {}

                    # --- push to the results table
                    tbl_results = self.apply_dual_selections(df_results, dual_selections, ALL_PREFIX,
                                                             reestablish_numbering=True, ordering=results_table_headings).values.tolist()
                    self.updateTable(ui, ui.main_window, '_TABLE_RESULTS_MAIN_', tbl_results)

                    # --- Update our main table title and descriptor
                    num_missing = len(df_results[df_results['Status'] == 'MISSING'])
                    num_matches = len(df_results) - num_missing
                    num_unmatched_queries = num_missing
                    num_matched_queries = len(queries) - num_missing
                    ui.set_txt(ui.main_window, '_LABEL_RESULTS_MAIN_', '    SEARCH Results')
                    descriptor = '[SEARCH] of [%s] using [%s]:  [%d] matched with [%d] results, [%d] unmatched.' % \
                                 (peer_info['peer_name'], os.path.basename(load_path), num_matched_queries, num_matches,
                                  num_unmatched_queries)
                    ui.set_txt(ui.main_window, '_DESCRIPTOR_MAIN_', descriptor)

                    # --- Save the find results to an excel file
                    splt = os.path.splitext(os.path.basename(load_path))
                    basename_no_ext = splt[0]
                    ext = splt[1]
                    find_results_fname = os.path.join(xlsx_dir, basename_no_ext + search_results_suffix + ext)
                    self.save_to_xlsx(ui, find_results_fname, df_results, file_descriptor='SEARCH RESULTS',
                                      create_dirs=True)

                    # --- Announce the good news
                    ui.popup('Search complete\n[%d] matched with [%d] results, [%d] unmatched.' % (
                    num_matched_queries, num_matches, num_unmatched_queries))

                    # --- advance the phase
                    ui.set_phase(ui.PHASE_FILT)


                elif key == '_T_MOVES_DONE_':
                    # ------------------- C-MOVES are all over~!
                    # if there is no C-MOVE dir, then something went catastrophically wrong and absolutely nothing
                    # got done. Don't even bother making a legend.
                    if not os.path.isdir(self_move_dir):
                        ui.set_phase(ui.PHASE_MOVE)
                        continue

                    # -- pull the resultant dataframe with updated statuses
                    df_results = val['df_results']

                    # -------------------------------- Create legend excel files documenting the C-MOVE results
                    # Generally regardless of how we exited the C-MOVE loop (error, cancelled, finished), we want to
                    # create a legend based on whatever information we do have currently.
                    headings_out = ['Study', 'Status', 'Query'] + legend_tags
                    basename_out = os.path.basename(self_move_dir)
                    dir_out = self_move_dir


                    # --- INITIAL QUERIES (e.g. the most basic user-passed-in queries used for the initial search),
                    # with associated automatically assigned query numbering that is propagated forward in the
                    # search/transfer result excel files
                    df_initial_queries = df_queries
                    fname_out = basename_out + initial_queries_suffix + '.xlsx'
                    path_out = os.path.join(dir_out, fname_out)
                    self.save_to_xlsx(ui, path_out, df_initial_queries, file_descriptor='INITIAL QUERIES')

                    # --- MASTER LEGEND/KEY for all entries
                    df_master_key_legend = df_results[headings_out]
                    fname_out = basename_out + legend_suffix + '.xlsx'
                    path_out = os.path.join(dir_out, fname_out)
                    self.save_to_xlsx(ui, path_out, df_master_key_legend, file_descriptor='MASTER KEY/LEGEND')

                    # --- IGNORED entries (primarily due to ANONYMIZATION)
                    df_ignored = df_results[df_results['Status'].str.startswith('IGNORED')][headings_out]
                    fname_out = basename_out + ignored_suffix + '.xlsx'
                    path_out = os.path.join(dir_out, fname_out)
                    self.save_to_xlsx(ui, path_out, df_ignored, file_descriptor='ENTRIES IGNORED DUE TO ANONYMIZATION')

                    # --- DESELECTED/EXCLUDED entries (due to manual user selection/deselection)
                    df_deselected = df_results[df_results['Status'].str.startswith('READY')][headings_out]
                    fname_out = basename_out + deselected_suffix + '.xlsx'
                    path_out = os.path.join(dir_out, fname_out)
                    self.save_to_xlsx(ui, path_out, df_deselected, file_descriptor='USER-DESELECTED ENTRIES')

                    # --- MISSING entries (two possibilities)
                    # (1) query not found in the peer's database, or
                    df_notfound = df_results[df_results['Status'].str.startswith('MISSING')][headings_out]
                    # (2) query found (which may or may not return multiple studies), but user filters/anonymization
                    # caused ZERO series from this given study to be downloaded. This overlaps with the "excluded
                    # entries" excel sheet
                    excluded_studyuids = set(
                        df_deselected['StudyInstanceUID'])
                    success_studyuids = set(
                        df_results.loc[df_results['Status'].str.startswith('DOWNLOADED'), 'StudyInstanceUID'])
                    fully_excluded_studyuids = excluded_studyuids.difference(success_studyuids)
                    df_fully_excluded = df_results.loc[df_results['StudyInstanceUID'].isin(fully_excluded_studyuids),
                                                   headings_out]

                    df_missing = pd.concat([df_notfound, df_fully_excluded])

                    fname_out = basename_out + missing_suffix + '.xlsx'
                    path_out = os.path.join(dir_out, fname_out)
                    self.save_to_xlsx(ui, path_out, df_missing, file_descriptor='MISSING ENTRIES')

                    # --- FAILED entries (found in the peer's database but either transfer or local storage failed)
                    df_failed = df_results[df_results['Status'].str.startswith('FAILED')][headings_out]
                    fname_out = basename_out + failed_suffix + '.xlsx'
                    path_out = os.path.join(dir_out, fname_out)
                    self.save_to_xlsx(ui, path_out, df_failed, file_descriptor='FAILED ENTRIES')

                    # --- SUCCESSFUL entries
                    df_success = df_results[headings_out]
                    df_success = df_success.merge(
                        pd.concat([df_ignored, df_deselected, df_missing, df_failed]),
                        how='left',
                        on=df_success.columns.values.tolist(),
                        indicator=True
                    )
                    df_success = df_success[df_success['_merge'] == 'left_only'].drop(columns=['_merge'])
                    fname_out = basename_out + success_suffix + '.xlsx'
                    path_out = os.path.join(dir_out, fname_out)
                    self.save_to_xlsx(ui, path_out, df_success, file_descriptor='SUCCESSFUL ENTRIES')

                    # 8. --- Update our table with all the move results
                    if sort_yes:
                        df_results = df_results.sort_values(by=sort_yes)
                    tbl_studies = self.apply_dual_selections(df_results, dual_selections, ALL_PREFIX,
                                                             reestablish_numbering=True, ordering=results_table_headings).values.tolist()
                    self.updateTable(ui, ui.main_window, '_TABLE_RESULTS_MAIN_', tbl_studies)

                    # 6. --- Update our main table title and descriptor
                    ui.set_txt(ui.main_window, '_LABEL_RESULTS_MAIN_', '    TRANSFER Results')
                    num_moves = val['num_moves']
                    descriptor = '[TRANSFER] from [%s] to [%s] using [%s]:  [%d/%d] matches updated.' % \
                                 (peer_info['peer_name'], dest_info['peer_name'], os.path.basename(load_path),
                                  num_moves,
                                  len(queries))
                    ui.set_txt(ui.main_window, '_DESCRIPTOR_MAIN_', descriptor)

                    ui.popup('Transfer complete\n%d/%d entries updated' % (num_moves, len(queries)))

                    # 9. --- Advance phase
                    ui.set_phase(ui.PHASE_DONE)



            # ---------------
            # MAIN TAB EVENTS
            # ---------------
            if event == '':
                # The first event upon opening the program will send an event == ''
                # Changing tabs will also send this event == ''
                pass

            elif event == '_LOAD_MAIN_':

                # --- ask what file to load
                path = ui.popupGetXlsxFile(load_path)
                if not path:
                    continue

                # --- Load excel file
                load_path = path
                df_queries_temp = self.load_excel(ui, load_path, query_table_headings, excel_to_tag)
                if df_queries_temp is None:
                    continue

                # --------- set 'Query' column numbering, so that we know which queries yielded which results.
                # User can set 'Query' column numbering themselves if they set an 'ID' or 'Query' or 'QueryID' column
                # in their input excel sheet
                # --- if user-defined 'Query' numbering column is already here, then just do a quick check that they
                # are all integers and sort the dataframe based on the query numbering
                if 'Query' in df_queries_temp.columns:
                    # --- sort based on the user-defined query numbering.
                    try:
                        df_queries_temp = df_queries_temp.sort_values(by=['Query'])
                    except TypeError as err:
                        ui.popupError('***ERROR***\nIncorrect formatting of user-defined \'Query\' column value: must be '
                                      'integer.\n%s\n%s\n%s' %
                                      (load_path, type(err).__name__, err))
                # --- If 'Query' column is invalid (e.g. either no user-defined query numbering column is found,
                # or the user-defined query numbering column is empty), then just assign query numbering in the
                # order the queries were read in.
                try:
                    print(df_queries_temp['Query'].astype(int))

                except ValueError as err:
                    df_queries_temp.index += 1
                    df_queries_temp['Query'] = df_queries_temp.index

                # --- standardize excel values to dataframe values as necessary
                try:
                    df_queries_temp = self.format_df(ui, df_queries_temp, master_tagname_to_tag,
                                                     self.parse_to_val_func,
                                                     self.parse_to_val_by_VR_func)
                except Exception as err:
                    traceback.print_stack()
                    ui.popupError('***ERROR***\nFailed to load file \'%s\'\n%s: %s' %
                                  (os.path.basename(load_path), type(err).__name__, err))
                    continue

                # --- lock in df_queries
                df_queries = df_queries_temp[query_table_headings]

                # --- Update UI
                tbl_raw = df_queries.values.tolist()
                self.updateTable(ui, ui.main_window, '_TABLE_RAW_MAIN_', tbl_raw)

                # --- Advance the stage
                ui.set_phase(ui.PHASE_FIND)


            elif event == '_LOAD_SEARCH_RESULTS_MAIN_':
                # --- ask what file to load
                path = ui.popupGetXlsxFile(load_path, title=('Load Input *%s.xlsx File' % search_results_suffix))
                if not path:
                    continue

                # --- Load excel file
                load_path = path
                df_results_temp = self.load_excel(ui, load_path, results_table_headings, excel_to_tag)
                if df_results_temp is None:
                    continue

                # --- standardize excel values to dataframe values as necessary
                try:
                    df_results_temp = self.format_df(ui, df_results_temp, master_tagname_to_tag,
                                                     self.parse_to_val_func,
                                                     self.parse_to_val_by_VR_func)
                except Exception as err:
                    traceback.print_stack()
                    ui.popupError('***ERROR***\nFailed to load file \'%s\'\n%s: %s' %
                                  (os.path.basename(load_path), type(err).__name__, err))
                    continue

                # --- lock in our new df_results dataframe
                df_results = df_results_temp

                # --- sort
                if sort_yes:
                    df_results = df_results.sort_values(by=sort_yes)

                # --- generate default study/series filters
                selections = self.generate_selections(df_results)
                dual_selections = {}

                # --- push to the UI results table
                tbl_results = self.apply_dual_selections(df_results, dual_selections, ALL_PREFIX,
                                                         reestablish_numbering=True, ordering=results_table_headings).values.tolist()
                self.updateTable(ui, ui.main_window, '_TABLE_RESULTS_MAIN_', tbl_results)

                # --- Update our main table title and descriptor
                num_missing = len(df_results[df_results['Status'] == 'MISSING'])
                num_matches = len(df_results) - num_missing
                ui.set_txt(ui.main_window, '_LABEL_RESULTS_MAIN_', '    SEARCH Results')
                descriptor = '[LOADED] from [%s]:  ' \
                             '[%d] search results, [%d] unmatched.' % \
                             (os.path.basename(load_path), num_matches, num_missing)
                ui.set_txt(ui.main_window, '_DESCRIPTOR_MAIN_', descriptor)

                # --- advance the phase
                ui.set_phase(ui.PHASE_FILT)

            elif event == '_FIND_MAIN_':
                if df_queries is None or len(df_queries) <= 0:
                    continue

                # --- Connection info
                peer_info = peers[values['_COMBO_SRC_MAIN_']]
                self_info = peers[self.SELF]

                # --- CREATE C-FIND QUERIES
                study_descr_key = '_EXACT_MATCH_STUDYDESCRIPTION_'
                series_descr_key = '_EXACT_MATCH_SERIESDESCRIPTION_'
                args = {study_descr_key: values[study_descr_key], series_descr_key: values[series_descr_key]}
                [queries, formatting_failures] = self.craft_queries(ui, df_queries, query_tags=query_tags_sorted,
                                                                    master_tagname_to_tag=master_tagname_to_tag, args=args)

                # --- set phase
                ui.set_phase(ui.PHASE_LOCK)

                # -----------------------------------------------------------------
                # THREADING C-FINDS
                # -----------------------------------------------------------------
                thread_stop = False
                thread_id = threading.Thread(target=self.threaded_perform_finds,
                                             args=(lambda: thread_stop,
                                                   pac,
                                                   ui_queue,
                                                   queries,
                                                   self_info,
                                                   peer_info),
                                             daemon=True)
                thread_id.start()

            # --- User-selected header tag filters for which studies/series we ultimately download
            elif event == '_FILT_MAIN_':

                if df_results is None or len(df_results) <= 0:
                    continue

                temp_dual_selections = ui.popupDataFrameDualSelector(ui,
                                                                     df_results,
                                                                     self.apply_dual_selections,
                                                                     dual_selections,
                                                                     independent_variable='StudyDescription',
                                                                     dependent_variable='SeriesDescription',
                                                                     all_prefix=ALL_PREFIX,
                                                                     enable_variable_selection=False)
                if temp_dual_selections is None:
                    continue

                dual_selections = temp_dual_selections
                tbl_results = self.apply_dual_selections(df_results, dual_selections, ALL_PREFIX,
                                                         reestablish_numbering=True, ordering=results_table_headings).values.tolist()
                self.updateTable(ui, ui.main_window, '_TABLE_RESULTS_MAIN_', tbl_results)

                ui.set_phase(ui.PHASE_MOVE)


            # DEPRECATED, NOT USED
            elif event == '_FILT_SIMPLE_MAIN_':

                if df_results is None or len(df_results) <= 0:
                    continue

                selected_heading = values['_COMBO_FILT_SIMPLE_MAIN_']
                [yes, no] = selections[selected_heading]

                df_filtered = self.apply_selections(df_results, selections, ignore={selected_heading})
                unique_filtered = df_filtered[selected_heading].unique()
                # combined, the lists 'yes' and 'no' will always contain all unique values in a column. However,
                # we will only display the relevant values given the current filter selections
                yes_enabled = []
                yes_disabled = []
                no_enabled = []
                no_disabled = []
                for y in yes:
                    if y in unique_filtered:
                        yes_enabled.append(y)
                    else:
                        yes_disabled.append(y)
                for n in no:
                    if n in unique_filtered:
                        no_enabled.append(n)
                    else:
                        no_disabled.append(n)

                yes_enabled_new, no_enabled_new = ui.popupSelector(ui,
                                                                   lst_available=no_enabled,
                                                                   lst_selected=yes_enabled,
                                                                   title=selected_heading + ' Selector',
                                                                   txt_available='Available ' + selected_heading,
                                                                   txt_selected='Selected ' + selected_heading)

                if yes_enabled_new is None or no_enabled_new is None:
                    continue

                yes_updated = yes_enabled_new + yes_disabled
                no_updated = no_enabled_new + no_disabled
                selections[selected_heading] = [yes_updated, no_updated]

                tbl_results = self.apply_selections(df_results, selections).values.tolist()
                self.updateTable(ui, ui.main_window, '_TABLE_RESULTS_MAIN_', tbl_results)

                ui.set_phase(ui.PHASE_MOVE)

            elif event == '_MOVE_MAIN_':
                if df_results is None or len(df_results) <= 0:
                    continue

                # 1. --- Only interested in entries that have been found and are not MISSING.
                # Drop all rows with (Status == 'MISSING')
                df_move = df_results.copy()
                df_move = df_move[df_move['Status'] != 'MISSING']
                # --- apply the HEADING SELECTIONS
                df_move = self.apply_dual_selections(df_move, dual_selections, ALL_PREFIX, reestablish_numbering=True,
                                                     ordering=results_table_headings)

                # 1. --- Connection info
                self_info = peers[self.SELF]
                peer_info = peers[values['_COMBO_SRC_MAIN_']]
                dest_info = peers[values['_COMBO_DEST_MAIN_']]

                # 2. --- C-MOVE modifiers
                # option to anonymize
                anonymize = values['_ANONYMIZE_MAIN_']
                # option to skip studies that are already on file (WILL ALWAYS BE FALSE, for now. No good way to
                # guarantee that a study/series on file is "complete" because we do not have access to total image
                # counts)
                skip_onfile = values['_SKIP_MAIN_']

                # 3. --- Create a dict of (studyuid -> studydirname) mappings
                # Create index mappings based on the unprocessed C-FIND results (df_results), so that indexing for any
                # given input xlsx file will be consistent, regardless of how the user filters their results
                studyuid_map = {}
                for _, row in df_results.iterrows():

                    # --- ignore entries that don't have studyUIDs
                    if row['StudyInstanceUID'] == '' or pd.isnull(row['StudyInstanceUID']):
                        continue

                    # ------- Generate all study folder names
                    if anonymize:
                        studydirname = str(row['Study'])
                    else:
                        mrn = pac.squish(row['PatientID'])
                        accession = pac.squish(row['AccessionNumber'])
                        try:
                            date = pd.to_datetime(row['StudyDate'].strftime('%Y-%m-%d'))
                        except (AttributeError, ValueError) as err:
                            date = pac.squish(row['StudyDate'])

                        studydirname = ('%s_%s_%s' % (mrn, date, accession))

                    # ------- MAPPING which will be used for 2 purposes
                    #   a) images from this study will be stored in this studydirname
                    #   b) if we are anonymizing, the studyUID in the image headers will be set to this value
                    studyuid_map[row['StudyInstanceUID']] = studydirname

                # 5. --- CRAFT C-MOVE QUERIES. Shotgun it.
                # assemble queries
                [queries, formatting_failures] = self.craft_queries(ui, df_move, query_tags=query_tags_sorted,
                                                                    master_tagname_to_tag=master_tagname_to_tag, args=None)
                # queries = [{'studyUID': x, 'PatientID': y} for (x, y) in zip(studyUIDs, mrns)]

                # 6. --- CONFIRM STUDY DOWNLOAD PATH, append numbers to the end of the name if necessary
                # Each downloaded study will have its own folder, and each of those study folders will go into
                # this main 'self_subdir' subdirectory which is named after its associated loaded excel file
                prefix = anon_prefix if anonymize else raw_prefix
                self_move_dir = os.path.join(storage_path,
                                             prefix + os.path.splitext(os.path.basename(load_path))[0])
                if os.path.isdir(self_move_dir):
                    dir_dirname = os.path.dirname(self_move_dir)
                    dir_basename = os.path.basename(self_move_dir)
                    self_move_dir = os.path.join(dir_dirname, dir_basename)
                    new_dir = self_move_dir
                    i = 1
                    # append numbers if necessary
                    while os.path.isdir(new_dir):
                        new_dir = '%s(%d)' % (self_move_dir, i)
                        i += 1
                    self_move_dir = new_dir

                # --- Save the initial move query parameters (aka the search results from the C-FIND) to an excel file
                move_queries_path = os.path.join(
                    self_move_dir, os.path.basename(self_move_dir) + search_results_suffix + '.xlsx')
                self.save_to_xlsx(ui, move_queries_path, df_results, file_descriptor='MOVE QUERIES',
                                  create_dirs=True)

                # --- set phase
                ui.set_phase(ui.PHASE_LOCK)

                # -----------------------------------------------------------------
                # THREADING C-MOVES
                # -----------------------------------------------------------------
                thread_stop = False
                thread_id = threading.Thread(target=self.threaded_perform_moves,
                                             args=(lambda: thread_stop,
                                                   pac,
                                                   ui_queue,
                                                   df_results,
                                                   queries,
                                                   self_info,
                                                   peer_info,
                                                   dest_info,
                                                   anonymize,
                                                   skip_onfile,
                                                   self_move_dir,
                                                   studyuid_map),
                                             daemon=True)
                thread_id.start()


            elif event == '_SORT_MAIN_':

                # --- make the options look pretty
                # fancy schmancy multipurpose selector popup! I'm very proud
                temp_sort_yes, temp_sort_no = ui.popupSelector(
                    ui=ui,
                    lst_available=sort_no,
                    lst_selected=sort_yes,
                    title='Sort Studies by...',
                    txt_available='Available Columns',
                    txt_selected='Sort by (Order matters)')

                # If user cancelled the sorting popup
                if temp_sort_yes is None or temp_sort_no is None:
                    continue

                # Update sorting criteria!
                sort_yes = temp_sort_yes
                sort_no = temp_sort_no

                # --- Sort!
                # only if there's something to sort
                if df_results is None or len(df_results) <= 0:
                    continue

                if sort_yes:
                    df_results = df_results.sort_values(by=sort_yes)
                # Update UI
                tbl_studies = self.apply_dual_selections(df_results, dual_selections, ALL_PREFIX,
                                                         reestablish_numbering=True, ordering=results_table_headings).values.tolist()
                self.updateTable(ui, ui.main_window, '_TABLE_RESULTS_MAIN_', tbl_studies)


            # ---------------
            # SETTINGS TAB EVENTS
            # ---------------
            elif event == '_BTN_LOCAL_CFG_':

                cancelled = False
                while True:
                    path = ui.popupGetFolder(title='Local Storage Directory',
                                             default_path=storage_path,
                                             initial_folder=storage_path if os.path.isdir(storage_path) else os.path.dirname(
                                                 storage_path)
                                             )
                    # cancelled
                    if not path:
                        cancelled = True
                        break
                    # valid folder
                    elif os.path.isdir(path):
                        break
                    # invalid folder
                    else:
                        ui.popupError('Invalid folder selection.')
                        continue

                if cancelled:
                    continue


                storage_path = path
                ui.set_txt(ui.main_window, '_DIR_LOCAL_CFG_', storage_path)


            elif event == '_BTN_LOAD_PEER_CFG_':
                peer_names = values['_LST_PEERS_CFG_']
                if not peer_names or not peer_names[0]:
                    continue
                peer_info = peers[peer_names[0]]
                self.display_selected_peer(ui, peer_info)

            elif event == '_BTN_SAVE_PEER_CFG_':
                peer_name = values['_NAME_PEER_CFG_'].strip()
                if not peer_name:
                    continue

                peer_info = {
                    'peer_name': peer_name,
                    'peer_aet': values['_AET_PEER_CFG_'].strip(),
                    'peer_ip': values['_IP_PEER_CFG_'].strip(),
                    'peer_port': values['_PORT_PEER_CFG_'].strip()
                }

                peers[peer_name] = peer_info
                src_selected = values['_COMBO_SRC_MAIN_']
                dest_selected = values['_COMBO_DEST_MAIN_']

                self.updatePeers(ui, peers, src_highlight=src_selected, dest_highlight=dest_selected, peer_highlights=[
                    peer_name])

                # save changes to file
                peers_path = config['config_peers']
                try:
                    self.save_to_json(peers_path, peers, overwrite=False)
                except Exception as err:
                    traceback.print_stack()
                    print('ERROR: Could not save Peers List to file (%s):  %s' % (peers_path, err))

            elif event == '_BTN_DELETE_PEER_CFG_':
                peer_names = values['_LST_PEERS_CFG_']
                if not peer_names or not peer_names[0] or peer_names[0] == self.SELF:
                    continue
                peers.pop(peer_names[0])

                src_selected = values['_COMBO_SRC_MAIN_']
                dest_selected = values['_COMBO_DEST_MAIN_']
                self.updatePeers(ui, peers, src_highlight=src_selected, dest_highlight=dest_selected)

                # save changes to file
                peers_path = config['config_peers']
                try:
                    self.save_to_json(peers_path, peers)
                except Exception as err:
                    traceback.print_stack()
                    print('ERROR: Could not save Peers List to file (%s):  %s' % (peers_path, err))



        ui.main_window.Close()


main = Main()
main.run()


