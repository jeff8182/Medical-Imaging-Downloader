import myGUI
import pacs
import datastruct
from datastruct import NodeStatus
from datastruct import RowStatus
from datastruct import Phase
from datastruct import Tag

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

    phases = [p for p in Phase]
    cur_phase = Phase.PHASE_CHOICE
    furthest_phase = Phase.PHASE_CHOICE
    new_project = True

    # --- Formatting functions
    # dicts containing functions for value formatting/mapping when we (1) initially load an excel file, (2) convert
    # dataframe values to tag values, and (3) convert tag values to dataframe values
    parse_to_tag_func = {}
    parse_to_val = {}
    excel_to_tag = {}
    master_tagname_to_tag = {}

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
        "default_prefix_anon": "ANON_",
        "default_prefix_raw": "RAW_",
        "default_suffix_initial_queries": "_SNAPSHOT_InitialQueries",
        "default_suffix_search_results": "_SNAPSHOT_InitialSearchResults",
        "default_suffix_success": "SuccessfulDownloads",
        "default_suffix_failure": "UnsatisfiedQueries",
        "default_suffix_allstudies": "AllResults_ByStudies",
        "default_suffix_allseries": "_SNAPSHOT_AllResults",
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
            'peer_aet': 'EXAMPLE_SELF',
            'peer_ip': '1.1.1.1',
            'peer_port': 4100,
        },
        'EXAMPLE PACS': {
            'peer_name': 'EXAMPLE PACS',
            'peer_aet': 'PACSQR',
            'peer_ip': '2.2.2.2',
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
        Tag.QueryNumber: ['ID', 'Query', 'Query ID', 'QueryID', 'Index', 'Query Number'],
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

        # --- starting phase
        self.cur_phase = Phase.PHASE_CHOICE

        # --- (dataframe value) to (tag), when we prep values to send a query
        self.parse_to_tag_func = {
            'PatientID': self.parse_mrn,
            'StudyDescription': self.parse_to_tag_studydescription,
            'SeriesDescription': self.parse_to_tag_seriesdescription
        }
        self.parse_to_tag_by_VR_func = {
            'DA': self.parse_to_tag_date,
            'TM': self.parse_to_tag_time,
            # 'DT': self.parse_to_tag_datetime
        }

        # --- (tag) to (dataframe value), to take query results and convert them back for display/storage purposes
        self.parse_to_val_func = {
            'PatientID': self.parse_mrn,
            Tag.QueryNumber: self.parse_pad_num,
            Tag.StudyNumber: self.parse_pad_num
        }
        self.parse_to_val_by_VR_func = {
            'DA': self.parse_to_val_date,
            'TM': self.parse_to_val_time,
            # 'DT': self.parse_to_val_datetime
        }

    # ----------------------- FUNCTIONS FOR FORMAT CONVERSIONS (dataframe value <-> DICOM Tag value)
    # both parse structures use the DATAFRAME representations of the tag names / column headings

    # --- Query numbering formatting
    def parse_pad_num(self, q_num, args=None):
        if not q_num or q_num == '' or q_num == '*' or pd.isna(q_num):
            return ''
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
        return dt  # return pd.to_datetime(dt).strftime(format)

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
    def save_to_xlsx(self, ui, dir_out, fname_out, df, file_descriptor='the', create_dirs=False, include_index=False):
        full_path = os.path.join(dir_out, fname_out)
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
            print('\'%s\': %s,' % (k, str(v)))

    # ONLY uses tags from pac.QUERY to craft the query, does not use any extraneous columns from the passed in dataframe. So
    # if you want to ask about a specific tag, it needs to be specified in your pac.QUERY template
    def craft_queries(self, ui, df, query_tags, query_identifier_col=Tag.QueryNumber, args=None):
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
                # --- Insert tags into the query. Only insert tags that exist in our predefined 'query_tags'
                val = str(row[tag]) if ((tag in row) and (not pd.isnull(row[tag]))) else ''
                vr = self.master_tagname_to_tag[tag][2] if tag in self.master_tagname_to_tag else None
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
        # Add additional keys for 'Status' and 'QueryNumber'
        #
        if not all_results:
            all_results = {Tag.RowStatus: [], Tag.QueryNumber: []}
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
                # match_i = '%d/%d' %(i, num_results)
                all_results[Tag.QueryNumber].append(query_identifier)

            # Update the status column
            all_results[Tag.RowStatus] += [RowStatus.FOUND] * num_results

        # --- Deal with queries that had 0 matches
        else:
            # We still want an entry in our results table if we didn't get any matches for a query - just use the original
            # query as one row
            for tag in query:
                all_results[tag].append(query[tag])

            # note the number of matches for this query
            all_results[Tag.QueryNumber].append(query_identifier)
            # Update the status column
            all_results[Tag.RowStatus].append(RowStatus.MISSING)

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

    def format_df_columns(self, df, parse_func={}, parse_by_vr_func={}):
        for heading in df.columns:
            try:
                vr = self.master_tagname_to_tag[heading][2] if heading in self.master_tagname_to_tag else None
                if vr in parse_by_vr_func and heading not in parse_func:
                    df.loc[:, heading] = \
                        df[heading].map(lambda x: parse_by_vr_func[vr](x, None))
                elif heading in parse_func:
                    df.loc[:, heading] = \
                        df[heading].map(lambda x: parse_func[heading](x, None))
            except ValueError as err:
                raise Exception('Issue formatting column: %s\n%s' % (heading, err))


        return df

    def format_df(self, ui, df, parse_func={}, parse_by_vr_func={}):
        # --------- General formatting
        df = self.format_df_general(df)
        # --- format individual columns
        df = self.format_df_columns(df, parse_func, parse_by_vr_func)
        return df

    def combine_with_numbering(self, ui, df_old, df_new, col_numbering):
        # --- isolate new rows that are not already in our existing row list
        df_old_nonumbering = df_old.drop([col_numbering], axis='columns')
        df_new_nonumbering = df_new.drop([col_numbering], axis='columns')
        df_onlynew = df_new_nonumbering[~df_new_nonumbering.apply(tuple, 1).isin(df_old_nonumbering.apply(tuple, 1))]

        # --- New rows will be assigned sequential numbers starting from after the largest existing query number. Any
        # current gaps in existing query numbering will intentionally NOT be filled.
        first_new_number = df_old[col_numbering].astype(int).max() + 1
        df_onlynew.insert(0,
                          col_numbering,
                          range(first_new_number, first_new_number + len(df_onlynew)))


        # --- standard dataframe formatting
        try:
            df_onlynew = self.format_df(ui,
                                        df_onlynew,
                                        self.parse_to_val_func,
                                        self.parse_to_val_by_VR_func)
        except Exception as err:
            traceback.print_stack()
            ui.popupError('***ERROR***\nFailed to assign query numbering to new queries:\n%s - %s' %
                          (type(err).__name__, err))
            return None

        # --- combine old + new
        df_combined = df_old.append(df_onlynew, sort=False).sort_values([col_numbering])

        return df_combined

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

    def establish_numbering(self, df, ordering, numbering_col=Tag.StudyNumber, unique_col='StudyInstanceUID'):

        old_index = 'old_index'
        df.loc[:, old_index] = df.index
        if numbering_col in df.columns:
            df = df.drop([numbering_col], 1)

        # --- select only 1 of each unique_col, but with the exception of also keep all rows that have unique_col==''
        df_index_by_study = df.loc[
            (~df[unique_col].duplicated()) |
            (df[unique_col] == ''),
            [old_index, unique_col]
        ]

        # --- set the numbering based on the index
        df_index_by_study = df_index_by_study.reset_index(drop=True)
        df_index_by_study.index += 1
        df_index_by_study[numbering_col] = df_index_by_study.index

        # --- convert the ints into 4-digit integer strings
        df_index_by_study.loc[:, numbering_col] = \
            df_index_by_study.loc[:, numbering_col].apply(lambda x: self.parse_pad_num(x))

        # --- Apply our established numbering to the main results dataframe
        df = df.merge(df_index_by_study, on=[old_index, unique_col], how='outer')
        df = df.drop(columns=[old_index])
        # --- propagate the numbering forward for any studies that span multiple rows (e.g. have more than 1
        # unique series identifier listed for a given study)
        df = df.fillna(method='ffill')
        df = df[ordering]

        return df

    def apply_dual_selections(self, df, dual_selections, all_prefix, ignore_independent_vals=[],
                              reestablish_numbering=False, ordering=None, numbering_col=Tag.StudyNumber, \
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

    def load_excel(self, ui, path, required_headings=[], sheet_name=None):
        try:
            sheets = pd.read_excel(path, sheet_name=sheet_name)
        except Exception as err:
            traceback.print_stack()
            ui.popupError('ERROR: Failed to read in "%s"\n%s' % (path, err))
            return None

        # --- Append each study of interest into a list of dicts, then display all valid entries
        # the DataFrame structure is the pandas library representation of a sheet in excel
        valid_sheets = {}
        # For now, ONLY GRAB THE FIRST SHEET
        for sheet_name in [list(sheets.keys())[0]]:
            try:
                df = sheets[sheet_name]
                # --- Try to standardize the excel column headers
                df = df.rename(columns=self.excel_to_tag)

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

    def load_queries(self, ui, load_path, query_table_headings, generate_missing_querynumbering=True):
        # --- Load excel file
        df_queries_temp = self.load_excel(ui, load_path, query_table_headings)
        if df_queries_temp is None:
            return None

        # --------- set 'QueryNumber' column numbering, so that we know which queries yielded which results.
        # User can include a 'QueryNumber' / 'Query' column in their input excel sheet to provide query numbering
        # themselves if they desire, or if they have some special query numbering system they wanted to go by.

        # --- if user-defined Tag.QueryNumber numbering column is already here, then just do a quick check that they
        # are all integers and are all valid. If they are valid, sort the dataframe based on the query numbering
        try:
            # --- check for existence of column and that they are all integers. Will throw an exception if either of
            # those criteria are not met
            df_queries_temp[Tag.QueryNumber].astype(int)

            # --- duplicate queryNumber numbers are not allowed
            if df_queries_temp[Tag.QueryNumber].duplicated().any():
                ui.popupError(
                    '***ERROR***\nDuplicate QueryNumber found. Each row in the input file represents one query, '
                    'and if a "QueryNumber" column is specified by the input file, each value in that column '
                    'must be a unique integer.\n\nFile:  %s\nDuplicate QueryNumbers:  %s' %
                    (load_path,
                     str(set(df_queries_temp[df_queries_temp[Tag.QueryNumber].duplicated(keep=False)][
                         Tag.QueryNumber].values.tolist()))))
                return None

            # --- sort based on the user-defined query numbering.
            try:
                df_queries_temp = df_queries_temp.sort_values(by=[Tag.QueryNumber])
            except TypeError as err:
                ui.popupError(
                    '***ERROR***\nIncorrect formatting of user-defined "QueryNumber" column value: must be '
                    'integer.\n%s\n%s\n%s' %
                    (load_path, type(err).__name__, err))

        except ValueError as err:

            # If 'QueryNumber' column is invalid (e.g. either no user-defined query numbering column is found
            # or the user-defined query numbering column is empty / contains non-integer values), then just assign
            # a default query numbering in the order the queries were read in.

            # if there is no querynumber column at all then generate a placeholder column, as the rest of the
            # program generally assumes it is present
            if Tag.QueryNumber not in df_queries_temp.columns:
                df_queries_temp[Tag.QueryNumber] = ''

            # --- Default query numbering based on the order the rows were provided in
            if generate_missing_querynumbering:
                ui.popup(
                    'Automatically generating query numbering.\n\nIf manual query numberings were '
                    'provided via a "QueryNumber" column,\nplease ensure that every row has a unique '
                    'integer value and that there are no blank rows.',
                    title='Automatically Generating Query Numbers'
                )

                # autogenerate based on the order the rows were provided
                df_queries_temp.index += 1
                df_queries_temp.loc[:, Tag.QueryNumber] = df_queries_temp.index


        # --------- standardize excel values to dataframe values as necessary
        try:

            df_queries_temp = self.format_df(ui, df_queries_temp,
                                             self.parse_to_val_func,
                                             self.parse_to_val_by_VR_func)
        except Exception as err:
            traceback.print_stack()
            ui.popupError('***ERROR***\nInvalid format in file \'%s\'\n%s: %s' %
                          (os.path.basename(load_path), type(err).__name__, err))
            return None

        # return result
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

    def updatePhase(self, ui, phase, reset_progression=False, args=None):
        self.cur_phase = phase
        if (self.cur_phase.value > self.furthest_phase.value) or reset_progression:
            self.furthest_phase = self.cur_phase

        ui.set_phase(self.cur_phase, self.furthest_phase, args=args)

    def updateTable(self, ui, window, key, tbl, headings=None):
        ui.set_table(window, key, tbl, headings)

    def updateTree(self, ui, window, key, study_nodes):
        return ui.set_tree(window, key, study_nodes)

    # WILL IGNORE ANY (Status == 'Missing') entries, because those are failed queries
    def parse_query_results_to_study_nodes(self, df):

        # do not display any unfound queries (represented by rows where (df[Tag.RowStatus] == 'MISSING')
        df = df[df[Tag.RowStatus] != RowStatus.MISSING]

        if df.empty:
            return [{}, {}]

        # aggregate all seriesdescriptions associated with each study
        df = df.sort_values(['StudyDescription', 'SeriesDescription', Tag.StudyNumber]).groupby(
            ['StudyDescription', Tag.StudyNumber])[
            'SeriesDescription'].agg(';'.join).reset_index()

        # sort and convert to a dict for parsing
        datadict = df[['StudyDescription', 'SeriesDescription', Tag.StudyNumber]]. \
            sort_values(['StudyDescription', 'SeriesDescription', Tag.StudyNumber]). \
            to_dict(orient='index')

        # track unique study-series keys, since we may encounter multiple duplicate series/study descriptions.
        unique_nodes = {}
        # for each study found, tally the unique combination of studydescription and seriesdescriptions
        study_nodes = {}
        for index in datadict.keys():
            row = datadict[index]
            studyseries_unique_key = 'STUDY:' + row['StudyDescription'] + '___ALLSERIES' + row['SeriesDescription']

            # --- if we have already encountered this unique study+allseries combo
            if studyseries_unique_key in study_nodes:
                study_nodes[studyseries_unique_key].num_similar_studyseries += 1
                study_nodes[studyseries_unique_key].study_numbers.append(row[Tag.StudyNumber])
            # --- if this is a new unique study+allseries combo
            else:
                # --- STUDY NODE
                # create study node
                study_node = datastruct.StudyDescriptionNode(row['StudyDescription'], studyseries_unique_key)
                study_node.study_numbers.append(row[Tag.StudyNumber])

                # --- SERIES NODES associated with this study node
                for seriesd in list(filter(None, row['SeriesDescription'].split(';'))):
                    series_unique_key = 'SERIES:' + seriesd + '___' + studyseries_unique_key
                    # create series node
                    series_node = datastruct.SeriesDescriptionNode(seriesd, series_unique_key)
                    study_node.add_series_node(series_node)
                    unique_nodes[series_unique_key] = series_node

                study_nodes[studyseries_unique_key] = study_node
                unique_nodes[studyseries_unique_key] = study_node

        return [study_nodes, unique_nodes]

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
                msg = 'WARNING: DO NOT CLICK THE "X".\nTO CANCEL, CLICK "CANCEL" AND THE TRANSFER WILL STOP AFTER ' \
                      'THE ' \
                      'CURRENT STUDY/SERIES IS COMPLETED.\n' \
                      'SOURCE:        %s\n' \
                      'DESTINATION:   %s\n' \
                      '\nTransferring %d out of %d...\n' \
                      '\nStudy #: %s\nStudy: %s\nSeries: %s' \
                      '\n\n\nCOMPLETED TRANSFERS:' % \
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
                                           Tag.RowStatus] = status
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

                    # --- If MOVE destination is NOT ourself, then we are not expected to receive any data
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
                # prefix = 'DOWNLOADED:' if message is None else message + ': '
                # counts = '%d/%d stored, %d/%d ignored' % (series_stored, study_stored, series_passed, study_passed)
                # status = prefix + counts
                status = message if message else RowStatus.DOWNLOADED
                df_results.loc[
                    (df_results['StudyInstanceUID'] == cur_studyuid) &
                    (df_results['SeriesInstanceUID'] == cur_seriesuid),
                    Tag.RowStatus] = status

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
        self.master_tagname_to_tag = self.load_json(config[tags_key],
                                                    default=self.DEFAULT_MASTER_TAGNAME_TO_TAGS,
                                                    sort_keys=True)
        # convert 'tag group' and 'tag element' hex strings into their corresponding integer values
        self.master_tagname_to_tag = {k: [int(grp, 0), int(elm, 0), vr]
                                      for k, [grp, elm, vr]
                                      in self.master_tagname_to_tag.items()}

        # --------- Load Study Identifier Tags
        # only use tagnames that do exist in the master list
        identifier_key = 'config_query_identifiers'
        identifier_wrapper_key = 'QUERY_IDENTIFIERS'
        query_identifiers = self.load_json(config[identifier_key],
                                           default=self.DEFAULT_QUERY_IDENTIFIERS,
                                           key=identifier_wrapper_key)
        # tagnames are valid only if they do exist in the master list
        query_identifiers = [x for x in query_identifiers if x in self.master_tagname_to_tag]

        # --------- Load Query Template
        query_key = 'config_query_tags'
        query_wrapper_key = 'QUERY_TAGS'
        query_tags_sorted = self.load_json(config[query_key],
                                           default=self.DEFAULT_QUERY_TAGS_SORTED,
                                           key=query_wrapper_key)
        # include identifier tags in the query as well
        query_tags_sorted = query_tags_sorted + [x for x in query_identifiers if x not in query_tags_sorted]
        # tagnames are valid only if they do exist in the master list
        query_tags_sorted = [x for x in query_tags_sorted if x in self.master_tagname_to_tag]

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
        self.excel_to_tag = {}
        for k, v in tag_to_excel.items():
            for heading in v:
                self.excel_to_tag[heading] = k
                self.excel_to_tag[heading.lower()] = k
                self.excel_to_tag[heading.upper()] = k
                self.excel_to_tag[heading.capitalize()] = k

        # --------- Load Miscellaneous User Parameters
        user_defaults_key = 'config_user_defaults'
        user_defaults = self.load_json(config[user_defaults_key], default=self.DEFAULT_USER_DEFAULTS)

        prefix_anon = user_defaults['default_prefix_anon']
        prefix_raw = user_defaults['default_prefix_raw']
        suffix_initial_queries = user_defaults['default_suffix_initial_queries']
        suffix_search_results = user_defaults['default_suffix_search_results']
        suffix_success = user_defaults['default_suffix_success']
        suffix_failure = user_defaults['default_suffix_failure']
        suffix_allstudies = user_defaults['default_suffix_allstudies']
        suffix_allseries = user_defaults['default_suffix_allseries']

        legend_tags = user_defaults['default_legend_tags']
        legend_tags = [x for x in legend_tags if x in self.master_tagname_to_tag]
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
        query_table_headings = [Tag.QueryNumber] + query_identifiers
        query_table_padding = [' ' * 6] * len(query_table_headings)

        # --- Table headings for the table displaying Query Results
        # Tag.RowStatus in particular requires a lot of space for text
        results_table_headings = [Tag.StudyNumber, Tag.RowStatus, Tag.QueryNumber] + query_tags_sorted
        results_table_padding = [''] + \
                                [' ' * 23] + \
                                ([' ' * 6] * (len(results_table_headings) - 2))

        # --- Results Tree representing unique study+series combinations
        study_nodes = {}
        unique_nodes = {}
        num_total_selected_series = 0

        # --- Current PHASE
        self.cur_phase = Phase.PHASE_CHOICE

        # --- Variables to track the current storage directory. Updated every time a download is triggered for the
        # first time after loading a new input xlsx file
        self_move_dir = None

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

        # --- Phase (broadly speaking, phase order goes LOAD, FIND, FILTER, MOVE)
        # There is a necessary chain of steps to go through to successfully transfer an image. If the chain is broken or
        # done out of order, it may need to reset
        self.updatePhase(ui, ui.first_phase)

        # *******************************
        # PAC (netcode, DICOM stuff)
        # *******************************
        pac = pacs.PAC(self.master_tagname_to_tag, query_tags_sorted, anon_imgs, anon_vrs, anon_tags, main=self)

        # *******************************
        # MAIN UI EVENT LOOP
        # *******************************
        while True:
            event, values = ui.main_window.Read(timeout=100)
            # print(event, values)

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
                        self.updatePhase(ui, Phase.PHASE_FIND)
                        continue

                    # ------------ Convert to dataframe and standardize
                    df_results_temp = pd.DataFrame(all_query_results)

                    # --- formatting (generalized column formatting + individual column formatting)
                    try:
                        df_results_temp = self.format_df(ui, df_results_temp,
                                                         self.parse_to_val_func,
                                                         self.parse_to_val_by_VR_func)
                    except Exception as err:
                        traceback.print_stack()
                        ui.popupError('***ERROR***\nFailed to parse C-FIND QUERIES:\n%s - %s' %
                                      (type(err).__name__, err))
                        self.updatePhase(ui, Phase.PHASE_FIND)
                        continue

                    # ------- Establish STUDY NUMBERING (which will double as ANONYMIZATION INDEXES)
                    df_results_temp = df_results_temp.sort_values(by=query_table_headings)
                    df_results_temp = self.establish_numbering(df_results_temp,
                                                               ordering=results_table_headings,
                                                               numbering_col=Tag.StudyNumber,
                                                               unique_col='StudyInstanceUID')



                    # --------- if we are reloading an existing project, then combine the new search results with our
                    # existing search results
                    if self.new_project:
                        # --- Lock in df_results
                        df_results = df_results_temp
                    else:
                        # --- Load a snapshot of the previous results (the AllResults SNAPSHOT file)
                        path_prevresults = os.path.join(self_move_dir, suffix_allseries + '.xlsx')
                        df_prevresults = self.load_excel(ui, path_prevresults, results_table_headings)

                        if df_prevresults is None:
                            ui.popupError('***ERROR***\nResults snapshot file "%s" could not be loaded.'
                                          'Cannot validate current search results with past search results.\n%s'
                                          % path_prevresults)
                        else:
                            try:
                                # --- standardize excel values to dataframe values as necessary
                                df_prevresults = self.format_df(ui, df_prevresults,
                                                                 self.parse_to_val_func,
                                                                 self.parse_to_val_by_VR_func)

                                # --- If we can successfully load the old results, merge them with the new results and assign
                                # numbering to the new studies as appropriate
                                # - isolate new rows that are not already in our existing row list
                                # - ignore StudyNumber permanently (we will be assigning new values)
                                df_prevresults_nonumbering = df_prevresults.drop([Tag.StudyNumber],
                                                                                 axis='columns')
                                df_results_temp_nonumbering = df_results_temp.drop([Tag.StudyNumber],
                                                                                   axis='columns')

                                # Statuses may also vary depending on the search/download status of the existing
                                # project, so ignore them temporarily when comparing new and old results
                                df_onlynewresults_nonumbering = df_results_temp_nonumbering[
                                    ~df_results_temp_nonumbering.drop([Tag.RowStatus], axis='columns').apply(tuple, 1).
                                    isin(
                                    df_prevresults_nonumbering.drop([Tag.RowStatus], axis='columns').apply(tuple, 1))]


                                # --- Assign new studynumbers to the new rows

                                # - get the max existing studynumber so we know where to continue the numbering
                                studynumber_offset = df_prevresults[Tag.StudyNumber].astype(int).max()
                                # - establish numbering within this set of new studies
                                df_onlynewresults = self.establish_numbering(df_onlynewresults_nonumbering,
                                                                             ordering=results_table_headings,
                                                                             numbering_col=Tag.StudyNumber,
                                                                             unique_col='StudyInstanceUID')
                                # - offset the studynumbering to account for the old studynumbers
                                df_onlynewresults[Tag.StudyNumber] = df_onlynewresults[
                                    Tag.StudyNumber].astype('int') + studynumber_offset
                                # - format the studynumbers
                                df_onlynewresults[Tag.StudyNumber] = \
                                    df_onlynewresults[Tag.StudyNumber].apply(lambda x: self.parse_pad_num(x))

                                # --- combine old + new
                                df_results = df_prevresults.append(df_onlynewresults, sort=False) \
                                    .sort_values([Tag.StudyNumber])

                            except Exception as err:
                                traceback.print_stack()
                                ui.popupError('***ERROR***\nResults snapshot file "%s" was incorrectly formatted. '
                                              'Cannot validate current search results with past search results.\n%s - %s' %
                                              (path_prevresults, type(err).__name__, err))


                    # --- Assert the proper column order
                    df_results = df_results[results_table_headings]

                    # --- sort
                    if sort_yes:
                        df_results = df_results.sort_values(by=sort_yes)


                    # --------- DONE MANIPULATING THE DATAFRAME. Now update UI/files

                    # --- Save the search queries to an excel file
                    # - INITIAL QUERIES (a lightly processed version of most basic user-passed-in queries used for
                    # the initial search), with associated automatically assigned query numbering that is propagated
                    # forward in the search/transfer result excel files
                    #basename_out = os.path.basename(self_move_dir)
                    fname_out = suffix_initial_queries + '.xlsx'
                    self.save_to_xlsx(ui,
                                      self_move_dir,
                                      fname_out,
                                      df_queries,
                                      file_descriptor='INITIAL SEARCH QUERIES',
                                      create_dirs=True)

                    # - Save the search results to an excel file
                    fname_out = suffix_allseries + '.xlsx'
                    self.save_to_xlsx(ui,
                                      self_move_dir,
                                      fname_out,
                                      df_results,
                                      file_descriptor='INITIAL SEARCH RESULTS',
                                      create_dirs=True)


                    # --- UI selector representation
                    [study_nodes, unique_nodes] = self.parse_query_results_to_study_nodes(df_results)
                    self.updateTree(ui, ui.main_window, '_TREE_RESULTS_MAIN_', study_nodes)
                    # we start with a blank slate, with no studies/series selected
                    num_total_selected_series = 0

                    # --- Update our results descriptor
                    num_queries = df_results[Tag.QueryNumber].nunique()
                    num_matched = df_results[df_results[Tag.RowStatus] == RowStatus.FOUND][Tag.QueryNumber].nunique()
                    num_missing = len(df_results[df_results[Tag.RowStatus] == RowStatus.MISSING])
                    num_matches = df_results[Tag.StudyNumber].nunique() - num_missing
                    descriptor = '%d Quer%s: %d matching stud%s, %d matched quer%s, %d unmatched quer%s' % \
                                 (num_queries, 'y' if num_queries == 1 else 'ies',
                                  num_matches, 'y' if num_matches == 1 else 'ies',
                                  num_matched, 'y' if num_matched == 1 else 'ies',
                                  num_missing, 'y' if num_missing == 1 else 'ies',)
                    ui.set_txt(ui.main_window, '_DESCRIPTOR_MAIN_', descriptor)

                    # --- Announce the good news
                    ui.popup('Search Complete\n\n'
                             '%d Quer%s:\n'
                             '%d matching stud%s found\n'
                             '%d matched quer%s\n'
                             '%d unmatched quer%s' % (
                        (num_queries, 'y' if num_queries == 1 else 'ies',
                         num_matches, 'y' if num_matches == 1 else 'ies',
                         num_matched, 'y' if num_matched == 1 else 'ies',
                         num_missing, 'y' if num_missing == 1 else 'ies',)
                    ))

                    # --- advance the phase
                    self.updatePhase(ui, Phase.PHASE_FILT)

                # triggered when the async C-MOVES have all completed
                elif key == '_T_MOVES_DONE_':
                    # ------------------- C-MOVES are all finished~!
                    # Each C-MOVE should be putting files into the C-MOVE directory, so if there is no C-MOVE
                    # directory, then something went catastrophically wrong and absolutely nothing
                    # got done.
                    if not os.path.isdir(self_move_dir):
                        self.updatePhase(ui, Phase.PHASE_MOVE)
                        continue

                    # -- pull the resultant dataframe with updated statuses
                    df_results = val['df_results']

                    # -------------------------------- Create legend/key excel files to document the C-MOVE results
                    # Generally regardless of how we exited the C-MOVE loop (error, cancelled, finished), we want to
                    # create a legend based on whatever information we do have currently.
                    headings_out = [Tag.StudyNumber, Tag.RowStatus, Tag.QueryNumber] + legend_tags
                    basename_out = os.path.basename(self_move_dir)

                    # --- FIND/MOVE RESULTS

                    # - squish seriesdescriptions into one row as necessary
                    df_output = df_results[headings_out]
                    headings_out_no_series = [x for x in headings_out if 'Series' not in x]
                    df_output_by_study = df_output.groupby(headings_out_no_series)['SeriesDescription'] \
                        .agg(';'.join).reset_index()

                    # - ALL results (SERIES level, essentially displaying every piece of data we have, as granular as
                    # we can get)
                    fname_out = suffix_allseries + '.xlsx'
                    self.save_to_xlsx(ui, self_move_dir, fname_out, df_results,
                                      file_descriptor='ALL RESULTS (EVERY SERIES)')

                    # - ALL results (STUDY level) (technically is further segregated by 'Status' in addition to
                    # Tag.StudyNumber)
                    fname_out = suffix_allstudies + '.xlsx'
                    self.save_to_xlsx(ui, self_move_dir, fname_out, df_output_by_study,
                                      file_descriptor='ALL RESULTS (EVERY STUDY)')

                    # - SUCCESSFUL downloads (STUDY level)
                    df_success = df_output_by_study[df_output_by_study[Tag.RowStatus] == RowStatus.DOWNLOADED]
                    fname_out = suffix_success + '.xlsx'
                    self.save_to_xlsx(ui, self_move_dir, fname_out, df_success,
                                      file_descriptor='SUCCESSFUL DOWNLOADS')

                    # - UNSUCCESSFUL queries (queries which have zero successful downloads associated with them)
                    # (may be due to incorrect query or manual user selection/deselection)
                    # P[P.email.isin(S.email) == False]
                    df_failure = df_output_by_study[df_output_by_study[Tag.RowStatus] != RowStatus.DOWNLOADED]
                    df_failure = df_failure[~df_failure[Tag.QueryNumber].isin(df_success[Tag.QueryNumber])]
                    fname_out = suffix_failure + '.xlsx'
                    self.save_to_xlsx(ui, self_move_dir, fname_out, df_failure,
                                      file_descriptor='QUERIES WITHOUT ASSOCIATED SUCCESSFUL DOWNLOADS')

                    # 6. --- Update our results descriptor
                    # num_moves = val['num_moves']
                    # descriptor = '[TRANSFER] from [%s] to [%s] using [%s]:  [%d/%d] matches updated.' % \
                    #              (peer_info['peer_name'], dest_info['peer_name'], os.path.basename(load_path),
                    #               num_moves,
                    #               len(queries))
                    # ui.set_txt(ui.main_window, '_DESCRIPTOR_MAIN_', descriptor)
                    num_moves = val['num_moves']
                    ui.popup('Transfer Finished\n%d/%d entries updated' % (num_moves, len(queries)))

                    # 9. --- Return to PHASE_MOVE, to clear the PHASE_LOCK
                    self.updatePhase(ui, Phase.PHASE_MOVE)

            # ---------------
            # MAIN TAB EVENTS
            # ---------------
            if event == '':
                # The first event upon opening the program will send an event == ''
                # Changing tabs will also send this event == ''
                pass

            elif event == ui.BUTTON_BACK:
                idx = self.phases.index(self.cur_phase)
                self.updatePhase(ui, self.phases[idx-1], args=self.new_project)

            elif event == ui.BUTTON_NEXT:
                idx = self.phases.index(self.cur_phase)
                self.updatePhase(ui, self.phases[idx+1], args=self.new_project)

            elif event == ui.BUTTON_NEW:
                self.new_project = True
                self.updatePhase(ui,
                                 Phase.PHASE_PARAMETERS,
                                 reset_progression=True,
                                 args=self.new_project)
                ui.set_txt(ui.main_window, '_TXT_LOADQUERIES_', '')
                ui.set_txt(ui.main_window, '_TXT_STORAGEDIR_', '')

            elif event == ui.BUTTON_OLD:
                self.new_project = False
                self.updatePhase(ui,
                                 Phase.PHASE_PARAMETERS,
                                 reset_progression=True,
                                 args=self.new_project)

                ui.set_txt(ui.main_window, '_TXT_LOADQUERIES_', '')
                ui.set_txt(ui.main_window, '_TXT_STORAGEDIR_', '')

            elif event == ui.BUTTON_QUERYFILE:
                path = ui.popupGetXlsxFile(load_path)

                if not path:
                    continue

                ui.set_txt(ui.main_window, '_TXT_LOADQUERIES_', path)

                # If loading a new query file, autogenerate a storage directory name based on the query file name.
                # Append numbers as necessary to get a valid new folder name.
                if self.new_project:
                    autogen_storagedir = storage_path if os.path.isdir(
                        storage_path) else os.path.dirname(
                        storage_path)
                    autogen_storagedir = os.path.join(autogen_storagedir, os.path.splitext(os.path.basename(path))[0])
                    if os.path.isdir(autogen_storagedir):
                        dir_dirname = os.path.dirname(autogen_storagedir)
                        dir_basename = os.path.basename(autogen_storagedir)
                        autogen_storagedir = os.path.join(dir_dirname, dir_basename)
                        new_dir = autogen_storagedir
                        i = 1
                        # append numbers if necessary
                        while os.path.isdir(new_dir):
                            new_dir = '%s(%d)' % (autogen_storagedir, i)
                            i += 1
                        autogen_storagedir = new_dir

                    ui.set_txt(ui.main_window, '_TXT_STORAGEDIR_', autogen_storagedir)


            elif event == ui.BUTTON_STORAGEDIR:
                path = ui.popupGetFolder(title='Project Storage Directory',
                                         default_path=storage_path,
                                         initial_folder=storage_path if os.path.isdir(
                                             storage_path) else os.path.dirname(
                                             storage_path)
                                         )

                if not path:
                    continue

                ui.set_txt(ui.main_window, '_TXT_STORAGEDIR_', path)

            elif event == ui.BUTTON_LOAD_QUERIES:
                temp_path = values['_TXT_LOADQUERIES_']
                temp_storage_dir = values['_TXT_STORAGEDIR_']
                if not temp_path or not temp_storage_dir:
                    ui.popupError('Please enter valid values for both "Query File" and "Project Storage Directory".')
                    continue

                load_path = temp_path
                self_move_dir = temp_storage_dir

                # When reloading an old project, DO NOT autogenerate missing query numbering for the new queries.
                # If the user provides their own query numberings, those will still be accepted and used as long as
                # they do not conflict with any existing query numberings in the old project.
                df_queries_temp = self.load_queries(ui, load_path, query_table_headings,
                                                    generate_missing_querynumbering=self.new_project)
                if df_queries_temp is None:
                    continue

                # --- lock in df_queries
                df_queries = df_queries_temp[query_table_headings]

                # --- If we are revisiting an EXISTING project, then we need to assimilate these new queries into our
                # existing list of queries.
                if not self.new_project:
                    # --- Load a snapshot of the previous queries (the InitialSearchQueries SNAPSHOT file)
                    path_prevqueries = os.path.join(self_move_dir, suffix_initial_queries + '.xlsx')
                    df_prevqueries = self.load_queries(ui, path_prevqueries, query_table_headings)

                    # --- If we can successfully load the old queries, merge them with the new queries and assign
                    # numbering to the new queries as appropriate
                    if df_prevqueries is None:
                        ui.popupError('***ERROR***\nSnapshot queries file "%s" was not found. Cannot validate current '
                                      'queries with past queries.' % path_prevqueries)
                    else:
                        df_prevqueries = df_prevqueries[query_table_headings]

                        # --- if new queries comes with numberings that do not intersect/conflict with old query
                        # numbering, we're set!
                        if ('' not in df_queries[Tag.QueryNumber].values) \
                                and not \
                                (set(df_queries[Tag.QueryNumber]).
                                        intersection(
                                    set(df_prevqueries[Tag.QueryNumber]))):

                            print(df_queries[Tag.QueryNumber].values.tolist())
                            print(df_prevqueries[Tag.QueryNumber].values.tolist())
                            # simply combine the two sets of queries and continue
                            df_queries = df_queries.append(
                                df_prevqueries
                            ).sort_values([Tag.QueryNumber])

                        # --- if the new queries have no numbering or have invalid numbering, then autogenerate query
                        # numbering for the new queries.
                        else:

                            temp_df_queries = self.combine_with_numbering(ui,
                                                                          df_prevqueries,
                                                                          df_queries,
                                                                          Tag.QueryNumber)
                            if temp_df_queries is not None:
                                df_queries = temp_df_queries


                # --- Update UI
                # don't display any data columns that are completely empty
                df_queries_valid = df_queries.replace('', np.nan).dropna(how='all', axis='columns').fillna('')
                tbl_queries = df_queries_valid.values.tolist()
                headings_queries = df_queries_valid.columns.tolist()
                self.updateTable(ui, ui.main_window, '_TABLE_RAW_MAIN_', tbl_queries, headings=headings_queries)

                # --- Advance the stage
                if tbl_queries:
                    self.updatePhase(ui, Phase.PHASE_FIND, reset_progression=True)

            elif event == ui.BUTTON_LOAD_RESULTS:
                # --- ask what file to load
                path = ui.popupGetXlsxFile(load_path, title=('Load Input *%s.xlsx File' % suffix_search_results))
                if not path:
                    continue

                # --- Load excel file
                load_path = path
                df_results_temp = self.load_excel(ui, load_path, results_table_headings)
                if df_results_temp is None:
                    continue

                # --- standardize excel values to dataframe values as necessary
                try:
                    df_results_temp = self.format_df(ui, df_results_temp,
                                                     self.parse_to_val_func,
                                                     self.parse_to_val_by_VR_func)
                except Exception as err:
                    traceback.print_stack()
                    ui.popupError('***ERROR***\nFailed to load file \'%s\'\n%s: %s' %
                                  (os.path.basename(load_path), type(err).__name__, err))
                    continue

                # --- lock in our new df_results dataframe
                df_results = df_results_temp
                df_queries = None

                # --- sort
                if sort_yes:
                    df_results = df_results.sort_values(by=sort_yes)

                [study_nodes, unique_nodes] = self.parse_query_results_to_study_nodes(df_results)
                self.updateTree(ui, ui.main_window, '_TREE_RESULTS_MAIN_', study_nodes)
                num_total_selected_series = 0

                # --- Update our results descriptor
                num_queries = df_results[Tag.QueryNumber].nunique()
                num_missing = len(df_results[df_results[Tag.RowStatus] == RowStatus.MISSING])
                num_matches = df_results[Tag.StudyNumber].nunique() - num_missing
                descriptor = '%d Quer%s: %d Matching stud%s, %d Unmatched quer%s' % \
                             (num_queries, 'y' if num_queries == 1 else 'ies',
                              num_matches, 'y' if num_matches == 1 else 'ies',
                              num_missing, 'y' if num_missing == 1 else 'ies',)
                ui.set_txt(ui.main_window, '_DESCRIPTOR_MAIN_', descriptor)

                # --- advance the phase
                self.updatePhase(ui, Phase.PHASE_FILT, reset_progression=True)

            elif event == ui.BUTTON_FIND:
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
                                                                    args=args)

                # --- set phase
                self.updatePhase(ui, Phase.PHASE_LOCK)

                # -----------------------------------------------------------------
                # THREADED C-FINDS
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

            # --- Checkbox Tree manual implementation using icons
            elif event == '_TREE_RESULTS_MAIN_':
                if self.cur_phase == Phase.PHASE_LOCK:
                    continue

                selected_row_key = ui.main_window.Element('_TREE_RESULTS_MAIN_').SelectedRows[0]
                # we only update the tree if the user clicks on a SERIES node and it is not already in the DOWNLOADED
                # status
                if (selected_row_key.startswith('SERIES:')) and (unique_nodes[selected_row_key].nodestatus !=
                                                                 NodeStatus.DOWNLOADED):
                    series_node = unique_nodes[selected_row_key]
                    study_node = unique_nodes[series_node.parent_study_key]
                    # update the backend representations of the series/study amalgamations
                    if series_node.nodestatus == NodeStatus.SELECTED:
                        series_node.nodestatus = NodeStatus.UNSELECTED
                        study_node.num_selected_series -= 1
                        num_total_selected_series -= 1
                    else:
                        series_node.nodestatus = NodeStatus.SELECTED
                        study_node.num_selected_series += 1
                        num_total_selected_series += 1

                    # update the icon for the current (series) node
                    ui.set_tree_series_node(ui.main_window, '_TREE_RESULTS_MAIN_', selected_row_key,
                                            series_node.nodestatus)

                    # update the icon for the parent (study) node
                    ui.set_tree_study_node(ui.main_window, '_TREE_RESULTS_MAIN_', series_node.parent_study_key,
                                           study_node.num_selected_series > 0)

                    # require at least 1 selected series before allowing the user to attempt a download
                    # self.updatePhase(ui, Phase.PHASE_MOVE if num_total_selected_series > 0 else Phase.PHASE_FILT)


            # --- User-selected header tag filters for which studies/series we ultimately download
            elif event == ui.BUTTON_FILTER:

                if df_results is None or len(df_results) <= 0:
                    continue

                if num_total_selected_series > 0:
                    self.updatePhase(ui, Phase.PHASE_MOVE)
                else:
                    txt0 = 'Please use the checkboxes to select at least one series to download.'

                    txt1 = 'A)  INDIVIDUAL STUDY RESULTS ARE NOT DISPLAYED. EACH EXPANDABLE ENTRY REPRESENTS A ' \
                           'CLUSTER OF STUDIES that all share a unique combination of both StudyDescription and ' \
                           'SeriesDescriptions. The "# of Studies" column indicates how many studies are in that ' \
                           'cluster. This format is to allow the user to efficiently sift through a large numbers of ' \
                           'query results to quickly select the studies (and in particular the series) of interest.'

                    txt2 = 'B)  BY DEFAULT, NO STUDIES/SERIES ARE SELECTED FOR DOWNLOAD. You must EXPAND ' \
                           'EACH STUDY CLUSTER and MANUALLY SELECT THE SPECIFIC SERIES that you wish to download ' \
                           'from each study cluster. This is to encourage the user to be mindful of their ' \
                           'bandwidth/storage usage and only download the specific series in the specific studies that ' \
                           'they need. At least one series must be selected to initiate a download'

                    title = '3. Select Series of Interest for Each Study'

                    ui.popupTextBox(txt0 + '\n\n' + txt1 + '\n\n' + txt2,
                                                                       title=title)

            elif event == ui.BUTTON_MOVE:
                if df_results is None or len(df_results) <= 0:
                    continue

                # 0. --- Filter which entries we will call a C-MOVE on. Only interested in entries that have been
                # confirmed to be in the database and are not MISSING.
                df_move = df_results.copy()
                # - Drop all rows with (Status == 'MISSING' or 'DOWNLOADED')
                df_move = df_move[(df_move[Tag.RowStatus] != RowStatus.MISSING) & (df_move[Tag.RowStatus] != RowStatus.DOWNLOADED)]
                # - target only the user's selected studydescription/seriesdescription combos from the ui results tree
                selected_dfs = [pd.DataFrame(columns=df_move.columns)]
                for study_key in study_nodes.keys():
                    study_node = study_nodes[study_key]
                    if study_node.num_selected_series > 0:
                        for series_key in study_node.series_nodes:
                            series_node = study_node.series_nodes[series_key]
                            if series_node.nodestatus == NodeStatus.SELECTED:
                                selected_dfs.append(
                                    df_move[
                                        (df_move['StudyDescription'] == study_node.study_description) &
                                        (df_move['SeriesDescription'] == series_node.series_description) &
                                        (df_move[Tag.StudyNumber].isin(study_node.study_numbers))
                                        ]
                                )

                df_move = pd.concat(selected_dfs, ignore_index=True)

                # 1. --- Connection info
                self_info = peers[self.SELF]
                peer_info = peers[values['_COMBO_SRC_MAIN_']]
                dest_info = peers[values['_COMBO_DEST_MAIN_']]

                # 2. --- C-MOVE modifiers

                # - option to anonymize
                anonymize = values['_ANONYMIZE_MAIN_']

                # - option to skip studies that are already on file (WILL ALWAYS BE FALSE, for now. No good way to
                # guarantee that a study/series on file is "complete" because we do not have access to total image
                # counts). DOES NOT WORK.
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
                        studydirname = str(row[Tag.StudyNumber])
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
                                                                    args=None)

                # 6. --- CONFIRM DOWNLOAD STORAGE DIRECTORY, append numbers to the end of the name if necessary
                # if check_storage_dir:
                #     check_storage_dir = False
                #
                #     prefix = prefix_anon if anonymize else prefix_raw
                #     self_move_dir = os.path.join(storage_path,
                #                                  prefix + os.path.splitext(os.path.basename(load_path))[0])
                #     if os.path.isdir(self_move_dir):
                #         dir_dirname = os.path.dirname(self_move_dir)
                #         dir_basename = os.path.basename(self_move_dir)
                #         self_move_dir = os.path.join(dir_dirname, dir_basename)
                #         new_dir = self_move_dir
                #         i = 1
                #         # append numbers if necessary
                #         while os.path.isdir(new_dir):
                #             new_dir = '%s(%d)' % (self_move_dir, i)
                #             i += 1
                #         self_move_dir = new_dir

                # Commented out because initial search results are already saved when the C-FINDS finish
                # (key: _T_FINDS_DONE_)
                # --- Save the initial search results from the C-FIND to an excel file
                # Each downloaded study will have its own folder, and each of those study folders will go into
                # # this main 'self_move_dir' directory which is named after its associated loaded excel file
                # self.save_to_xlsx(ui,
                #                   self_move_dir,
                #                   os.path.basename(self_move_dir) + suffix_search_results + '.xlsx',
                #                   df_results,
                #                   file_descriptor='MOVE QUERIES',
                #                   create_dirs=True)

                # --- set phase
                self.updatePhase(ui, Phase.PHASE_LOCK)

                # -----------------------------------------------------------------
                # THREADED C-MOVES
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

            # deprecated. SORT BUTTON not used anymore.
            # elif event == '_SORT_MAIN_':
            #
            #     # --- make the options look pretty
            #     # fancy schmancy multipurpose selector popup! I'm very proud
            #     temp_sort_yes, temp_sort_no = ui.popupSelector(
            #         ui=ui,
            #         lst_available=sort_no,
            #         lst_selected=sort_yes,
            #         title='Sort Studies by...',
            #         txt_available='Available Columns',
            #         txt_selected='Sort by (Order matters)')
            #
            #     # If user cancelled the sorting popup
            #     if temp_sort_yes is None or temp_sort_no is None:
            #         continue
            #
            #     # Update sorting criteria!
            #     sort_yes = temp_sort_yes
            #     sort_no = temp_sort_no
            #
            #     # --- Sort!
            #     # only if there's something to sort
            #     if df_results is None or len(df_results) <= 0:
            #         continue
            #
            #     if sort_yes:
            #         df_results = df_results.sort_values(by=sort_yes)
            #     # Update UI
            #     tbl_studies = self.apply_dual_selections(df_results, dual_selections, ALL_PREFIX,
            #                                              reestablish_numbering=True,
            #                                              ordering=results_table_headings).values.tolist()
            #     self.updateTable(ui, ui.main_window, '_TABLE_RESULTS_MAIN_', tbl_studies)


            # ---------------
            # SETTINGS TAB EVENTS
            # ---------------
            elif event == '_BTN_LOCAL_CFG_':

                cancelled = False
                while True:
                    path = ui.popupGetFolder(title='Local Storage Directory',
                                             default_path=storage_path,
                                             initial_folder=storage_path if os.path.isdir(
                                                 storage_path) else os.path.dirname(
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
