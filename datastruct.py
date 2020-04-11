import enum

class NodeStatus(enum.Enum):
    SELECTED = 'SELECTED for Download'
    DOWNLOADED = 'DOWNLOADED'
    UNSELECTED = 'Ready'

class RowStatus:
    MISSING = 'MISSING'
    FOUND = 'FOUND'
    DOWNLOADED = 'DOWNLOADED'


class Tag:
    # tags we use locally to track queries and studies
    StudyNumber = 'StudyNumber'
    QueryNumber = 'QueryNumber'
    RowStatus = 'Status'

class SeriesDescriptionNode:
    nodestatus = False
    series_description = None
    unique_key = None
    parent_study_key = None

    def __init__(self, series_description, unique_key, nodestatus=NodeStatus.UNSELECTED):
        self.series_description = series_description
        self.unique_key = unique_key
        self.nodestatus = nodestatus


class StudyDescriptionNode:
    num_selected_series = 0
    study_description = None
    num_similar_studyseries = 1
    series_nodes = {}
    # study_numbers being a list instead of a set explicitly allows for the same study to be downloaded again if the
    # multiple different queries return the same study
    study_numbers = []
    unique_key = None

    def __init__(self, study_description, unique_key, is_selected=False):
        self.study_description = study_description
        self.series_nodes = {}
        self.study_numbers = []
        self.num_similar_studyseries = 1
        self.unique_key = unique_key
        self.is_selected = is_selected
        self.num_selected_series = 0

    def add_series_node(self, series_node, is_selected=False):
        if is_selected:
            self.num_selected_series +=1

        series_node.parent_study_key = self.unique_key

        self.series_nodes[series_node.unique_key] = series_node

class Phase(enum.Enum):
    PHASE_LOCK = 0
    PHASE_CHOICE = 1
    PHASE_PARAMETERS = 2
    PHASE_FIND = 3
    PHASE_FILT = 4
    PHASE_MOVE = 5
    PHASE_DONE = 6
