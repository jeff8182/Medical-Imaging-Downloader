class StudyDescriptionNode:
    isSelected = False
    studyDescription = None
    count = 1
    series_nodes = {}

    def __init__(self, accession, study_description, is_selected=False):
        self.accession = accession
        self.studyDescription = study_description

        self.isSelected = is_selected

    def add_series_node(self, series_description, is_selected=False):
        if is_selected:
            self.isSelected = True

        self.series_nodes[series_description] = is_selected



