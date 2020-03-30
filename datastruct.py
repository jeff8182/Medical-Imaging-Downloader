import enum

class Status(enum.Enum):
    SELECTED = 'SELECTED for Download'
    DOWNLOADED = 'DOWNLOADED'
    UNSELECTED = 'Ready'




class SeriesDescriptionNode:
    status = False
    series_description = None
    unique_key = None
    parent_study_key = None

    def __init__(self, series_description, unique_key, status=Status.UNSELECTED):
        self.series_description = series_description
        self.unique_key = unique_key
        self.status = status


class StudyDescriptionNode:
    num_selected_series = 0
    study_description = None
    num_similar_studyseries = 1
    series_nodes = {}
    unique_key = None
    status = None

    def __init__(self, study_description, unique_key, status, is_selected=False):
        self.study_description = study_description
        self.series_nodes = {}
        self.num_similar_studyseries = 1
        self.unique_key = unique_key
        self.is_selected = is_selected
        self.num_selected_series = 0
        self.status = status

    def add_series_node(self, series_node, is_selected=False):
        if is_selected:
            self.num_selected_series +=1

        series_node.parent_study_key = self.unique_key

        self.series_nodes[series_node.unique_key] = series_node



