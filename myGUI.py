import sys, os
if sys.version_info[0] >= 3:
    import PySimpleGUI as sg
else:
    import PySimpleGUI27 as sg

import textwrap

from datastruct import NodeStatus
from datastruct import Phase

class GUI:

    # *******************************
    # MAIN WINDOW
    # *******************************
    main_window = None

    # *******************************
    # POPUP LAYOUTS (SELECTOR, ADVANCED SELECTOR)
    # *******************************
    selector_layout = None
    advanced_selector_layout = None

    # *******************************
    # ELEMENT KEYS
    # *******************************

    # --- high level element groupings that can be made visible/invisible depending on the phase
    DISPLAY_RESULTS = '_DISPLAY_RESULTS_'
    DISPLAY_QUERIES = '_DISPLAY_QUERIES_'
    DISPLAY_NAVIGATION = '_DISPLAY_NAVIGATION_'
    DISPLAY_CHOICE = '_DISPLAY_CHOICE_'
    DISPLAY_PARAMETERS = '_DISPLAY_PARAMETERS_'
    DISPLAY_TITLE_LOAD = '_TITLE_LOAD_'
    DISPLAY_TXT_QUERYFILE = '_TXT_QUERYFILE_'
    DISPLAY_TXT_LOADDIR = '_TXT_LOADDIR_'
    DISPLAY_FIND = '_DISPLAY_FIND_'
    DISPLAY_FILTER = '_DISPLAY_FILTER_'
    DISPLAY_MOVE = '_DISPLAY_MOVE_'

    # --- individual buttons
    BUTTON_NEW = '_NEW_MAIN_'
    BUTTON_OLD = '_OLD_MAIN_'
    BUTTON_QUERYFILE = '_QUERYFILE_'
    BUTTON_STORAGEDIR = '_STORAGEDIR_'
    BUTTON_LOAD_QUERIES = '_LOAD_QUERIES_'
    BUTTON_LOAD_RESULTS = '_LOAD_SEARCH_RESULTS_MAIN_'
    BUTTON_FIND = '_FIND_MAIN_'
    BUTTON_FILTER = '_FILTER_MAIN_'
    BUTTON_MOVE = '_MOVE_MAIN_'
    BUTTON_NEXT = '_NEXT_MAIN_'
    BUTTON_BACK = '_BACK_MAIN_'


    # *******************************
    # PHASES
    # *******************************

    # phase limits for navigation (back/next)
    first_phase = Phase.PHASE_CHOICE
    last_phase = Phase.PHASE_FILT

    # Dynamic strings for the "LOAD PARAMETERS" phase
    str_loaddir_new = None
    str_loaddir_old = None
    str_loadqueryfile_new = None
    str_loadqueryfile_old = None
    str_loaddir_new = None
    str_loaddir_old = None


    # *******************************
    # COLORS
    # *******************************

    # default colors
    default_btn_back = 'darkblue'
    default_btn_text = 'white'
    default_btn_color = (default_btn_text, default_btn_back)
    highlight_btn_back = 'green1'
    highlight_btn_text = 'black'
    highlight_btn_color = (highlight_btn_text, highlight_btn_back)


    # *******************************
    # ICONS
    # *******************************

    # green check mark

    icon_greencheck = 'assets/greencheck16.png'
    icon_blackdash = 'assets/blackdash16.png'
    icon_blackdashthin = 'assets/blackdashthin16.png'
    icon_blackdashthick = 'assets/blackdashthick16.png'
    icon_blackcross = 'assets/blackcross16.png'
    icon_blackcrossthick = 'assets/blackcrossthick16.png'
    icon_redcross = 'assets/redcross16.png'
    icon_checkboxcheckedgreen = 'assets/checkboxcheckedgreen16.png'
    icon_checkboxempty = 'assets/checkboxempty16.png'
    icon_smallcheckboxcheckedgreen = 'assets/smallcheckboxcheckedgreen16.png'
    icon_smallcheckboxempty = 'assets/smallcheckboxempty16.png'

    # not selected


    # *******************************
    # FUNCTIONS
    # *******************************

    def OneLineProgressMeter(self, title, i, max_value, key, msg=None):
        return sg.OneLineProgressMeter(title, i, max_value, key, msg) if max_value > 0 else False

    def set_listbox(self, window, key, lst, highlights=None):
        idx_highlights = [lst.index(x) for x in highlights] if highlights else highlights
        window.Element(key).Update(lst, set_to_index=idx_highlights)

    def set_listbox_highlight(self, window, key, highlights):
        window.Element(key).SetValue(highlights)

    def set_txt(self, window, key, txt):
        window.Element(key).Update(txt)

    def set_combo(self, window, key, lst, select=None, enabled=True):
        window.Element(key).Update(select, lst, disabled=not enabled)

    def set_input(self, window, key, txt):
        window.Element(key).Update(txt)

    def update_table_with_headings(self, table_widget, values=None, num_rows=None, visible=None, select_rows=None,
                                headings=None):
        """
        Changes some of the settings for the Table Element. Must call `Window.Read` or `Window.Finalize` prior

        :param values:
        :param num_rows:
        :param visible: (bool) control visibility of element
        :param select_rows:
        """

        headings = ['a', 'b', 'c', 'd']

        if headings is not None:
            for ii, nn in enumerate(headings):
                table_widget.heading(ii, text=nn)

        if values is not None:
            children = table_widget.get_children()
            for i in children:
                table_widget.detach(i)
                table_widget.delete(i)
            children = table_widget.get_children()
            # self.TKTreeview.delete(*self.TKTreeview.get_children())
            for i, value in enumerate(values):
                if self.DisplayRowNumbers:
                    value = [i + self.StartingRowNumber] + value
                id = table_widget.insert('', 'end', text=i, iid=i + 1, values=value, tag=i % 2)
            if self.AlternatingRowColor is not None:
                table_widget.tag_configure(1, background=self.AlternatingRowColor)
            self.Values = values
            self.SelectedRows = []
        if visible is False:
            table_widget.pack_forget()
        elif visible is True:
            table_widget.pack()
        if num_rows is not None:
            table_widget.config(height=num_rows)
        if select_rows is not None:
            rows_to_select = [i + 1 for i in select_rows]
            table_widget.selection_set(rows_to_select)

    def set_table(self, window, key, tbl, headings=None):
        table_element = window.Element(key)

        # PySimpleGUI DOES NOT SUPPORT UPDATING TABLE HEADINGS ON THE FLY, so we need to access the underlying TK
        # widget and do it manually
        if headings:
            table_element.Widget["displaycolumns"] = headings
            #print(table_element.Widget.get_children())
            #for ii, nn in enumerate(headings):
            #    table_element.Widget.heading(ii, text=nn)

        table_element.Update(tbl)

    def set_tree_series_node(self, window, treekey, nodekey, status):
        tree_element = window.Element(treekey)

        switcher = {
            NodeStatus.SELECTED: self.icon_smallcheckboxcheckedgreen,
            NodeStatus.UNSELECTED: self.icon_smallcheckboxempty,
            NodeStatus.DOWNLOADED: self.icon_redcross
        }
        icon = switcher[status]

        tree_element.Update(
            key=nodekey,
            icon=icon,
            value=[]
        )

    def set_tree_study_node(self, window, treekey, nodekey, is_selected):
        tree_element = window.Element(treekey)

        tree_element.Update(
            key=nodekey,
            icon=self.icon_greencheck if is_selected else self.icon_blackcross
        )

    def set_tree(self, window, key, study_nodes):
        tree_element = window.Element(key)

        # --- create UI tree
        treedata = sg.TreeData()

        # track unique keys, since we may encounter multiple duplicate series/study descriptions
        unique_keys = {}

        # studyNodes is a list of unique combinations of one study description and its associated series descriptions
        for study_key in study_nodes.keys():
            study_node = study_nodes[study_key]

            # THIS STUDY
            treedata.Insert(
                parent='',
                key=study_node.unique_key,
                text=study_node.study_description,
                values=[str(study_node.num_similar_studyseries) +
                        ' Stud' +
                        ('y' if study_node.num_similar_studyseries == 1 else 'ies')],
                icon=(self.icon_greencheck if study_node.num_selected_series > 0 else self.icon_blackcross)
            )

            # for each unique series description in each unique study description
            series_switcher = {
                NodeStatus.SELECTED: self.icon_smallcheckboxcheckedgreen,
                NodeStatus.UNSELECTED: self.icon_smallcheckboxempty,
                NodeStatus.DOWNLOADED: self.icon_redcross
            }
            for series_unique_key in sorted(study_node.series_nodes.keys()):
                series_node = study_node.series_nodes[series_unique_key]
                # series within the study
                treedata.Insert(
                    parent=study_node.unique_key,
                    key=series_node.unique_key,
                    text=series_node.series_description,
                    values=[],
                    icon=(series_switcher[series_node.nodestatus])
                )

        tree_element.Update(treedata)
        return treedata

    def setVisible(self, window, key, visible):
        window.Element(key).Update(visible=visible)

    def enableButton(self, window, key, enabled):
        disabled = not enabled
        window.Element(key).Update(disabled=disabled)

    def colorButton(self, window, key, color=(default_btn_text, default_btn_back)):
        # color is a tuple: (text_color, button_color) ('white' on 'darkblue')
        window.Element(key).Update(button_color=color)


    def set_phase_CHOICE(self, args):
        # display
        self.setVisible(self.main_window, self.DISPLAY_CHOICE, True)

        self.setVisible(self.main_window, self.DISPLAY_PARAMETERS, False)
        self.setVisible(self.main_window, self.DISPLAY_QUERIES, False)
        self.setVisible(self.main_window, self.DISPLAY_FIND, False)
        self.setVisible(self.main_window, self.DISPLAY_RESULTS, False)
        self.setVisible(self.main_window, self.DISPLAY_FILTER, False)
        self.setVisible(self.main_window, self.DISPLAY_MOVE, False)
        # color
        self.colorButton(self.main_window, self.BUTTON_NEW, self.highlight_btn_color)
        self.colorButton(self.main_window, self.BUTTON_OLD, self.highlight_btn_color)
        # enable/disable
        self.enableButton(self.main_window, self.BUTTON_NEW, True)
        self.enableButton(self.main_window, self.BUTTON_OLD, True)

    def set_phase_PARAMETERS(self, new_project):
        # display
        self.setVisible(self.main_window, self.DISPLAY_PARAMETERS, True)
        self.set_txt(self.main_window, self.DISPLAY_TITLE_LOAD,
                     self.str_loadparam_new if new_project else self.str_loadparam_old)
        self.set_txt(self.main_window, self.DISPLAY_TXT_QUERYFILE,
                     self.str_loadqueryfile_new if new_project else self.str_loadqueryfile_old)
        self.set_txt(self.main_window, self.DISPLAY_TXT_LOADDIR,
                     self.str_loaddir_new if new_project else self.str_loaddir_old)

        self.setVisible(self.main_window, self.DISPLAY_CHOICE, False)
        self.setVisible(self.main_window, self.DISPLAY_QUERIES, False)
        self.setVisible(self.main_window, self.DISPLAY_FIND, False)
        self.setVisible(self.main_window, self.DISPLAY_RESULTS, False)
        self.setVisible(self.main_window, self.DISPLAY_FILTER, False)
        self.setVisible(self.main_window, self.DISPLAY_MOVE, False)
        # color
        self.colorButton(self.main_window, self.BUTTON_LOAD_QUERIES, self.highlight_btn_color)
        # enable/disable
        self.enableButton(self.main_window, self.BUTTON_LOAD_QUERIES, True)

    def set_phase_FIND(self, args):
        # display
        self.setVisible(self.main_window, self.DISPLAY_QUERIES, True)
        self.setVisible(self.main_window, self.DISPLAY_FIND, True)

        self.setVisible(self.main_window, self.DISPLAY_CHOICE, False)
        self.setVisible(self.main_window, self.DISPLAY_PARAMETERS, False)
        self.setVisible(self.main_window, self.DISPLAY_RESULTS, False)
        self.setVisible(self.main_window, self.DISPLAY_FILTER, False)
        self.setVisible(self.main_window, self.DISPLAY_MOVE, False)
        # color
        self.colorButton(self.main_window, self.BUTTON_FIND, self.highlight_btn_color)
        # enable/disable
        self.enableButton(self.main_window, self.BUTTON_FIND, True)

    def set_phase_FILT(self, args):
        # display
        self.setVisible(self.main_window, self.DISPLAY_RESULTS, True)
        self.setVisible(self.main_window, self.DISPLAY_FILTER, True)

        self.setVisible(self.main_window, self.DISPLAY_CHOICE, False)
        self.setVisible(self.main_window, self.DISPLAY_PARAMETERS, False)
        self.setVisible(self.main_window, self.DISPLAY_QUERIES, False)
        self.setVisible(self.main_window, self.DISPLAY_FIND, False)
        self.setVisible(self.main_window, self.DISPLAY_MOVE, False)
        # color
        self.colorButton(self.main_window, self.BUTTON_FILTER, self.highlight_btn_color)
        # enable/disable
        self.enableButton(self.main_window, self.BUTTON_FILTER, True)

    def set_phase_MOVE(self, args):
        # display
        self.setVisible(self.main_window, self.DISPLAY_MOVE, True)

        self.setVisible(self.main_window, self.DISPLAY_CHOICE, False)
        self.setVisible(self.main_window, self.DISPLAY_PARAMETERS, False)
        self.setVisible(self.main_window, self.DISPLAY_QUERIES, False)
        self.setVisible(self.main_window, self.DISPLAY_FIND, False)
        self.setVisible(self.main_window, self.DISPLAY_RESULTS, True)
        self.setVisible(self.main_window, self.DISPLAY_FILTER, False)
        # color
        self.colorButton(self.main_window, self.BUTTON_MOVE, self.highlight_btn_color)
        # enable/disable
        self.enableButton(self.main_window, self.BUTTON_MOVE, True)

    def set_phase_LOCK(self, args):
        # enable/disable
        self.enableButton(self.main_window, self.BUTTON_NEW, False)
        self.enableButton(self.main_window, self.BUTTON_OLD, False)
        self.enableButton(self.main_window, self.BUTTON_LOAD_QUERIES, False)
        self.enableButton(self.main_window, self.BUTTON_FIND, False)
        self.enableButton(self.main_window, self.BUTTON_FILTER, False)
        self.enableButton(self.main_window, self.BUTTON_MOVE, False)

    def set_phase(self, phase, furthest_phase, args=None):
        # --- Display different elements depending on the phase
        switcher = {
            Phase.PHASE_CHOICE: self.set_phase_CHOICE,
            Phase.PHASE_PARAMETERS: self.set_phase_PARAMETERS,
            Phase.PHASE_FIND: self.set_phase_FIND,
            Phase.PHASE_FILT: self.set_phase_FILT,
            Phase.PHASE_MOVE: self.set_phase_MOVE,
            Phase.PHASE_LOCK: self.set_phase_LOCK
        }
        switcher[phase](args)

        # --- Navigation buttons depend on the phase

        # - Enable/disable navigation
        # special phases (e.g. PHASE_LOCK) are hidden in front of self.first_phase, so navigation can never
        # accidentally get there
        self.main_window.Element(self.BUTTON_BACK).Update(disabled=
                                                          (phase.value <= self.first_phase.value))
        self.main_window.Element(self.BUTTON_NEXT).Update(disabled=
                                                          (phase.value >= furthest_phase.value) or
                                                          (phase.value < self.first_phase.value) or
                                                          (phase.value >= self.last_phase.value))



    # *******************************
    # POPUP FUNCTIONS
    # *******************************

    def popup(self, txt, title=None, keep_on_top=True):
        sg.Popup(txt, title=title, keep_on_top=keep_on_top)

    def popupTextBox(self, txt, title=None, keep_on_top=True):
        popupWindow = sg.Window(
            title=title,
            layout=[
                [sg.Multiline(default_text=txt, size=(60, None))],
                [sg.Button('Ok', key='_POPUP_OK_')]
            ],
            keep_on_top=keep_on_top
        ).Finalize()
        while True:
            event, values = popupWindow.Read()
            if event is None or event == 'Exit' or event == '_POPUP_OK_':
                break

        popupWindow.Close()


    def popupError(self, txt, keep_on_top=True):
        self.popup(txt, title='Error', keep_on_top=keep_on_top)

    def popupYesNo(self, txt, title=None, keep_on_top=True):
        return sg.PopupYesNo(txt, title=title, keep_on_top=keep_on_top)

    def popupGetXlsxFile(self, load_path, title='Load Input *.xlsx File'):
        while True:
            path = self.popupGetFile(title=title,
                                     message='',
                                     file_types=(('Excel Files', '.xlsx'),),
                                     default_path=load_path,
                                     initial_folder=load_path if os.path.isdir(load_path) else os.path.dirname(
                                         load_path)
                                     )
            # cancelled
            if not path:
                break
            # valid *.xlsx file
            elif os.path.isfile(path) and path.endswith('.xlsx'):
                break
            # invalid *.xlsx file
            else:
                self.popupError('Invalid selection. Please select an *.xlsx file.')

        return path

    def popupGetFile(self, title, message='', default_path='', initial_folder=None, file_types=(("ALL Files", "."),), multiple_files=False,
                     save_as=False):
        return sg.PopupGetFile(title=title,
                               message=message,
                               default_path=default_path,
                               initial_folder=initial_folder,
                               file_types=file_types,
                               multiple_files=multiple_files,
                               save_as=save_as
                               )

    def popupGetFolder(self, title, default_path='', initial_folder=None):
        return sg.PopupGetFolder(title, default_path=default_path, initial_folder=initial_folder)


    def createPopupSelector(self,
                            lst_available,
                            lst_selected=[],
                            title='Selector',
                            txt_available='Unused Options',
                            txt_selected='Selected Options',
                            sort_available=True,
                            sort_selected=False,
                            keep_on_top=True):

        # --- Create the selector window
        selector_window = sg.Window(
            title=title,
            layout=self.createSelectorLayout(),
            keep_on_top=keep_on_top
        ).Finalize()

        # --- Set initial values
        self.set_txt(selector_window, '_TXT_ALL_SEL_', txt_available)
        self.set_txt(selector_window, '_TXT_SEL_SEL_', txt_selected)

        # available list always sorted
        if sort_available:
            lst_available = sorted(lst_available)
        self.set_listbox(selector_window, '_LST_ALL_SEL_', lst_available)
        self.set_listbox_highlight(selector_window, '_LST_ALL_SEL_', [])

        # selected list has user-defined order
        if sort_selected:
            lst_selected = sorted(lst_selected)
        self.set_listbox(selector_window, '_LST_SEL_SEL_', lst_selected)
        self.set_listbox_highlight(selector_window, '_LST_SEL_SEL_', [])

        return selector_window

    def popupSelector(self,
                      ui,
                      lst_available,
                      lst_selected=[],
                      title='Selector',
                      txt_available='Available Options',
                      txt_selected='Selected Options',
                      sort_available=True,
                      sort_selected=False):
        # If the same value is found in both the 'selected' and 'available' lists, we will default it to the
        # 'selected' list
        lst_selected = [str(x) for x in lst_selected]
        lst_available = [str(x) for x in lst_available if x not in lst_selected]

        temp_available = lst_available.copy()
        temp_selected = lst_selected.copy()

        # --- Create selector window
        selector_window = self.createPopupSelector(temp_available,
                                                   lst_selected=temp_selected,
                                                   title=title,
                                                   txt_available=txt_available,
                                                   txt_selected=txt_selected,
                                                   sort_available=sort_available,
                                                   sort_selected=sort_selected)

        while True:                 # Event Loop
            update_available = False
            update_selected = False
            highlight_available = None
            highlight_selected = None

            event, values = selector_window.Read()
            # print(event, values)

            if event is None or event == 'Exit' or event == '_BTN_CANCEL_SEL_':
                final_selected = None
                final_available = None
                break

            elif event == '_BTN_OK_SEL_':
                final_selected = temp_selected
                final_available = temp_available
                break

            elif event == '_BTN_UP_SEL_':
                lst_up = values['_LST_SEL_SEL_']
                if not lst_up:
                    continue
                idx_up = [i for i, e in enumerate(temp_selected) if e in set(lst_up)]
                for i in idx_up:
                    if i > 0 and temp_selected[i - 1] not in lst_up:
                        above_val = temp_selected[i - 1]
                        temp_selected[i - 1] = temp_selected[i]
                        temp_selected[i] = above_val
                update_selected = True
                highlight_selected = lst_up

            elif event == '_BTN_DOWN_SEL_':
                lst_down = values['_LST_SEL_SEL_']
                if not lst_down:
                    continue
                idx_down = [i for i, e in enumerate(temp_selected) if e in set(lst_down)]
                idx_down.reverse()
                for i in idx_down:
                    if i < (len(temp_selected) - 1) and temp_selected[i + 1] not in lst_down:
                        below_val = temp_selected[i + 1]
                        temp_selected[i + 1] = temp_selected[i]
                        temp_selected[i] = below_val
                update_selected = True
                highlight_selected = lst_down

            elif event == '_BTN_ADD_SEL_':
                lst_add = values['_LST_ALL_SEL_']
                if not lst_add:
                    continue
                temp_available = [x for x in temp_available if x not in lst_add]
                temp_selected += lst_add
                update_available = True
                update_selected = True
                highlight_available = []
                highlight_selected = lst_add

            elif event == '_BTN_REMOVE_SEL_':
                lst_remove = values['_LST_SEL_SEL_']
                if not lst_remove:
                    continue
                temp_available += lst_remove
                temp_selected = [x for x in temp_selected if x not in lst_remove]
                update_available = True
                update_selected = True
                highlight_available = lst_remove
                highlight_selected = []

            elif event == '_BTN_ADDALL_SEL_':
                if not temp_available:
                    continue
                temp_selected += temp_available
                temp_available = []
                update_available = True
                update_selected = True
                highlight_available = []
                highlight_selected = []

            elif event == '_BTN_REMOVEALL_SEL_':
                if not temp_selected:
                    continue
                temp_available += temp_selected
                temp_selected = []
                update_available = True
                update_selected = True
                highlight_available = []
                highlight_selected = []

            elif event == '_BTN_RESTORE_SEL_':
                temp_available = lst_available.copy()
                temp_selected = lst_selected.copy()
                update_available = True
                update_selected = True
                highlight_available = []
                highlight_selected = []

            # update after every event. 'available' list is always sorted
            if update_available:
                if sort_available:
                    temp_available = sorted(temp_available)
                ui.set_listbox(selector_window, '_LST_ALL_SEL_', temp_available,
                               highlights=highlight_available)

            # 'selected' list has user-defined order
            if update_selected:
                if sort_selected:
                    temp_selected = sorted(temp_selected)
                ui.set_listbox(selector_window, '_LST_SEL_SEL_', temp_selected,
                               highlights=highlight_selected)

        selector_window.Close()

        # Return all values in the
        return final_selected, final_available

    def initializePopupAdvancedSelector(self,
                                        variables=[],
                                        independent_available=[],
                                        independent_available_highlights=None,
                                        independent_default=None,
                                        independent_txt='All selected',
                                        dependent_default=None,
                                        dependent_available=[],
                                        dependent_selected=[],
                                        title='Advanced Selector',
                                        txt_available='Available Options',
                                        txt_selected='Selected Options',
                                        sort_available=True,
                                        sort_selected=True,
                                        enable_variable_selection=True,
                                        keep_on_top=True):

        # --- Create the dual selector window
        advanced_selector_window = sg.Window(
            title=title,
            layout=self.createAdvancedSelectorLayout(),
            keep_on_top=keep_on_top
        ).Finalize()

        # --------- Set initial INDEPENDENT values
        # --- label
        self.set_txt(advanced_selector_window, '_TXT_VAL_FIRST_SEL_', independent_txt)
        # --- independent variables
        self.set_combo(advanced_selector_window, '_COMBO_FIRST_SEL_', variables, select=independent_default)
        # --- list of unique values for the selected independent variable
        self.set_listbox(advanced_selector_window, '_LST_FIRST_SEL_', independent_available,
                         highlights=independent_available_highlights)


        # --------- Set initial DEPENDENT values
        # --- dependent variables
        self.set_combo(advanced_selector_window, '_COMBO_SECOND_SEL_', variables, select=dependent_default)
        # --- labels
        self.set_txt(advanced_selector_window, '_TXT_ALL_SEL_', txt_available)
        self.set_txt(advanced_selector_window, '_TXT_SEL_SEL_', txt_selected)
        # --- list of available options
        if sort_available:
            dependent_available = sorted(dependent_available)
        self.set_listbox(advanced_selector_window, '_LST_ALL_SEL_', dependent_available)
        self.set_listbox_highlight(advanced_selector_window, '_LST_ALL_SEL_', [])
        # --- list of selected options
        if sort_selected:
            dependent_selected = sorted(dependent_selected)
        self.set_listbox(advanced_selector_window, '_LST_SEL_SEL_', dependent_selected)
        self.set_listbox_highlight(advanced_selector_window, '_LST_SEL_SEL_', [])


        if not enable_variable_selection:
            advanced_selector_window.Element('_COMBO_FIRST_SEL_').Update(disabled=True)
            advanced_selector_window.Element('_COMBO_SECOND_SEL_').Update(disabled=True)

        return advanced_selector_window


    def popupDataFrameDualSelector(self,
                                   ui,
                                   df,
                                   apply_dual_selections,
                                   dual_selections={},
                                   variables=None,
                                   independent_variable=None,
                                   independent_txt=None,
                                   dependent_variable=None,
                                   title='Advanced Selector',
                                   txt_available=None,
                                   txt_selected=None,
                                   sort_available=True,
                                   sort_selected=True,
                                   all_prefix='*ALL',
                                   enable_variable_selection=True):


        # ------------- INITIAL PARAMETERS

        # ------ Parameters that are NOT AFFECTED by selection filters

        # --- INDEPENDENT and DEPENDENT variable defaults
        if independent_variable is None:
            independent_variable = variables[0]
        if dependent_variable is None:
            dependent_variable = variables[1]

        # --- List of all variable/column names
        ALL = (all_prefix + (' %ss' % independent_variable)).upper()
        if variables is None:
            variables = [ALL] + sorted(df.columns.values.tolist())
        else:
            variables = [ALL] + sorted(variables)


        # ------ Parameters that YES ARE AFFECTED by selection filters

        # --- APPLY THE DUAL SELECTION FILTERS
        df_selected = apply_dual_selections(df, dual_selections, all_prefix=all_prefix)

        # --- values for the selected INDEPENDENT variable
        independent_available = [ALL] + sorted(list(df[independent_variable].unique()))
        independent_val = independent_available[0]
        independent_available_highlights = [independent_val]


        # --- available/selected values for the selected DEPENDENT variable
        if independent_variable in dual_selections and dependent_variable in dual_selections[independent_variable]:
            dependent_available = dual_selections[independent_variable][dependent_variable]['no']
            dependent_selected = dual_selections[independent_variable][dependent_variable]['yes']
        else:
            dependent_available = []
            dependent_selected = list(df_selected[dependent_variable].unique())


        # Preserve the original values, just in case user cancels
        temp_available = dependent_available.copy()
        temp_selected = dependent_selected.copy()
        temp_dual_selections = dual_selections.copy()

        # --- Labels
        if independent_txt is None:
            independent_txt = 'Selected %s Values' % independent_variable
        if txt_available is None:
            txt_available = 'Available %s Values' % dependent_variable
        if txt_selected is None:
            txt_selected = 'Selected %s Values' % dependent_variable


        # ------------- Create advanced selector window
        advanced_selector_window = self.initializePopupAdvancedSelector(
            variables=variables,
            independent_available=independent_available,
            independent_available_highlights=independent_available_highlights,
            independent_default=independent_variable,
            independent_txt=independent_txt,
            dependent_default=dependent_variable,
            dependent_available=dependent_available,
            dependent_selected=dependent_selected,
            title=title,
            txt_available=txt_available,
            txt_selected=txt_selected,
            sort_available=sort_available,
            sort_selected=sort_selected,
            enable_variable_selection=enable_variable_selection,
            keep_on_top=True)


        # ------------- Event loop
        while True:
            update_independent = False
            update_available = False
            update_selected = False
            highlight_available = None
            highlight_selected = None

            event, values = advanced_selector_window.Read()
            #print('--- %s ---' % event)

            # --- Exit advanced selector popup
            if event is None or event == 'Exit' or event == '_BTN_CANCEL_SEL_':
                final_dual_selections = None
                break

            # --- INDEPENDENT variable events
            elif event == '_COMBO_FIRST_SEL_':
                # TO BE IMPLEMENTED LOL. low priority.
                pass
            elif event == '_COMBO_FIRST_SEL_':
                # TO BE IMPLEMENTED LOL. low priority.
                pass
            elif event == '_LST_FIRST_SEL_':
                # BUGGED IN THIS EVENT? TEMP_SELECTED BECOMES BLANK
                independent_val = values['_LST_FIRST_SEL_'][0]

                df_selected = apply_dual_selections(df, temp_dual_selections, all_prefix=all_prefix,
                                                    ignore_independent_vals=[independent_val])
                #print(independent_val)
                #print('\tdf_selected unique: ', df_selected[dependent_variable].unique())
                update_available = True
                update_selected = True

                if independent_variable in temp_dual_selections and \
                        independent_val in temp_dual_selections[independent_variable] and \
                        dependent_variable in temp_dual_selections[independent_variable][independent_val]:


                    temp_available = temp_dual_selections[independent_variable][independent_val][dependent_variable]['no']
                    temp_selected = temp_dual_selections[independent_variable][independent_val][dependent_variable]['yes']
                    #print('dict entry exists')
                    #print('\t', temp_available)
                    #print('\t', temp_selected)

                # --- if no previous filtering for this independent/dependent/variable/value combination
                else:
                    temp_available = []
                    if independent_val == ALL:
                        temp_selected = list(df[dependent_variable].unique())
                    else:
                        temp_selected = list(
                            df.loc[df[independent_variable] == independent_val, dependent_variable].unique())
                    #print('NEW dict entry')
                    #print('\t', temp_available)
                    #print('\t', temp_selected)

            # --- DEPENDENT variable events
            elif event == '_BTN_OK_SEL_':
                final_dual_selections = temp_dual_selections
                break

            elif event == '_BTN_UP_SEL_':
                lst_up = values['_LST_SEL_SEL_']
                if not lst_up:
                    continue
                idx_up = [i for i, e in enumerate(temp_selected) if e in set(lst_up)]
                for i in idx_up:
                    if i > 0 and temp_selected[i - 1] not in lst_up:
                        above_val = temp_selected[i - 1]
                        temp_selected[i - 1] = temp_selected[i]
                        temp_selected[i] = above_val
                update_selected = True
                highlight_selected = lst_up

            elif event == '_BTN_DOWN_SEL_':
                lst_down = values['_LST_SEL_SEL_']
                if not lst_down:
                    continue
                idx_down = [i for i, e in enumerate(temp_selected) if e in set(lst_down)]
                idx_down.reverse()
                for i in idx_down:
                    if i < (len(temp_selected) - 1) and temp_selected[i + 1] not in lst_down:
                        below_val = temp_selected[i + 1]
                        temp_selected[i + 1] = temp_selected[i]
                        temp_selected[i] = below_val
                update_selected = True
                highlight_selected = lst_down

            elif event == '_BTN_ADD_SEL_':
                lst_add = values['_LST_ALL_SEL_']
                if not lst_add:
                    continue
                temp_available = [x for x in temp_available if x not in lst_add]
                temp_selected += lst_add
                update_available = True
                update_selected = True
                highlight_available = []
                highlight_selected = lst_add

            elif event == '_BTN_REMOVE_SEL_':
                lst_remove = values['_LST_SEL_SEL_']
                if not lst_remove:
                    continue
                temp_available += lst_remove
                temp_selected = [x for x in temp_selected if x not in lst_remove]
                update_available = True
                update_selected = True
                highlight_available = lst_remove
                highlight_selected = []

            elif event == '_BTN_ADDALL_SEL_':
                if not temp_available:
                    continue
                temp_selected += temp_available
                temp_available = []
                update_available = True
                update_selected = True
                highlight_available = []
                highlight_selected = []

            elif event == '_BTN_REMOVEALL_SEL_':
                if not temp_selected:
                    continue
                temp_available += temp_selected
                temp_selected = []
                update_available = True
                update_selected = True
                highlight_available = []
                highlight_selected = []

            elif event == '_BTN_RESTORE_SEL_':
                temp_available = dependent_available.copy()
                temp_selected = dependent_selected.copy()
                update_available = True
                update_selected = True
                highlight_available = []
                highlight_selected = []

            # --------- update UI after every event.

            # --- selected independent variable values
            if update_independent:
                independent_available = [ALL] + sorted(independent_available)

            # --- available dependent variable values
            if update_available:
                ui_temp_available = [x for x in temp_available if x in list(df_selected[dependent_variable].unique())]
                #print('\t\tavailable:', temp_available, '\n\t\tui_available', ui_temp_available)
                if sort_available:
                    ui_temp_available = sorted(temp_available)
                ui.set_listbox(advanced_selector_window, '_LST_ALL_SEL_', ui_temp_available,
                               highlights=highlight_available)

            # --- selected dependent variable values
            if update_selected:
                ui_temp_selected = [x for x in temp_selected if x in list(df_selected[dependent_variable].unique())]
                #print('\t\tselected:', temp_selected, '\n\t\tui_selected', ui_temp_selected)
                if sort_selected:
                    ui_temp_selected = sorted(ui_temp_selected)
                ui.set_listbox(advanced_selector_window, '_LST_SEL_SEL_', ui_temp_selected,
                               highlights=highlight_selected)

            # --- If both need updating, that means that values were moved between the two, and thus our
            # dual_selections should be updated as well
            # * I am ashamed of writing such obtuse code, but if it works it works. This is one of the few times
            # where the long variable names gets a little too ridiculous
            if update_available and update_selected:
                # if we already have a selection filter entry for this combo
                if independent_variable in temp_dual_selections and \
                        independent_val in temp_dual_selections[independent_variable] and \
                        dependent_variable in temp_dual_selections[independent_variable][independent_val]:

                    # grab the old filter entry
                    old_full_available = \
                        temp_dual_selections[independent_variable][independent_val][dependent_variable]['no']
                    old_full_selected = \
                        temp_dual_selections[independent_variable][independent_val][dependent_variable]['yes']

                    # update to the new filter entry, make sure there are no overlaps
                    updated_full_available = list(set(old_full_available).difference(set(temp_selected)) |
                                                  set(temp_available))
                    updated_full_selected = list(set(old_full_selected).difference(set(temp_available)) |
                                                 set(temp_selected))

                    temp_dual_selections[independent_variable][independent_val][dependent_variable]['no'] = \
                        updated_full_available
                    temp_dual_selections[independent_variable][independent_val][dependent_variable]['yes'] = \
                        updated_full_selected

                # If we do not have a selection filter entry for this combo, create one as necessary
                else:
                    if independent_variable not in temp_dual_selections:
                        temp_dual_selections[independent_variable] = {}
                    if independent_val not in temp_dual_selections[independent_variable]:
                        temp_dual_selections[independent_variable][independent_val] = {}
                    if dependent_variable not in temp_dual_selections[independent_variable][independent_val]:
                        temp_dual_selections[independent_variable][independent_val][dependent_variable] = {}

                    temp_dual_selections[independent_variable][independent_val][dependent_variable]['no'] = \
                        temp_available

                    temp_dual_selections[independent_variable][independent_val][dependent_variable]['yes'] = \
                        temp_selected


                    #set(df.loc[df[independent_variable] == independent_val, dependent_variable].unique(
                    #)).difference(set(temp_available))

                # If the 'no' list is empty (meaning we are essentially not filtering anything with our yes/no
                # selection), # then just drop that entry from the selections dict
                if not temp_dual_selections[independent_variable][independent_val][dependent_variable]['no']:
                    temp_dual_selections[independent_variable][independent_val].pop(dependent_variable)
                    if not temp_dual_selections[independent_variable][independent_val]:
                        temp_dual_selections[independent_variable].pop(independent_val)
                        if not temp_dual_selections[independent_variable]:
                            temp_dual_selections.pop(independent_variable)


        advanced_selector_window.Close()

        # Return all values in the
        return final_dual_selections


    def spacer(self, sz=(1, 1)):
        return sg.Text('', sz)


    # *******************************
    # Create the UI window
    # *******************************
    def createMainWindow(self,
                         headings_pretty_raw_main,
                         headings_pretty_results_main,
                         padding_pretty_raw_main=None,
                         padding_pretty_results_main=None,
                         default_filter_main=None):
        # *******************************
        # UI Elements
        # *******************************

        # -------------------------- TAB: Main

        # ------ TABLE: raw entries loaded from a file
        title_querydatabase = sg.Text('SEARCH DATABASE', text_color='darkblue', font='Any 18 bold')
        text_table_raw_main = sg.Text('  Studies of Interest (Queries)', text_color='darkblue', font='Any 11')
        raw_padding = padding_pretty_raw_main if padding_pretty_raw_main else [' ' * 6] * len(headings_pretty_raw_main)
        table_raw_main = sg.Table(
            values=[raw_padding],
            headings=headings_pretty_raw_main,
            select_mode='extended',
            num_rows=14,
            alternating_row_color='#ffffff',
            vertical_scroll_only=True,
            hide_vertical_scroll=True,
            justification='left',
            key='_TABLE_RAW_MAIN_'
        )
        coltable_raw_main = sg.Column([[table_raw_main]],
                                      size=(575, None),
                                      scrollable=True
                                      )
        col_table_raw_main = sg.Column(
            [
                [title_querydatabase],
                [text_table_raw_main],
                [coltable_raw_main]
            ],
            visible=False,
            key=self.DISPLAY_QUERIES
        )

        # ------ TABLE: query results
        title_downloadstudies = sg.Text('DOWNLOAD STUDIES', text_color='darkblue', font='Any 18 bold')
        text_table_results_main = sg.Text('  Query Results', text_color='darkblue', font='Any 11',
                                          key='_LABEL_RESULTS_MAIN_')
        btn_sort_main = sg.Button('Sort',
                                  size=(9, 1),
                                  key='_SORT_MAIN_')
        btn_load_find_results_main = sg.Button('Load Snapshot',
                                  size=(12, 1),
                                  button_color=self.default_btn_color,
                                  visible=False,
                                  key=self.BUTTON_LOAD_RESULTS)
        text_descriptor_main = sg.Text('', size=(55, 1), key='_DESCRIPTOR_MAIN_')

        #results_padding = padding_pretty_results_main if padding_pretty_results_main else [''] * len(
        #    headings_pretty_results_main)
        filler_treedata = sg.TreeData()
        filler_treedata.Insert(
            parent='',
            key='_',
            text='',
            values=[],
        )
        tree_results_main = sg.Tree(
            data=filler_treedata,
            headings=['# of Studies'],
            col0_width=36,
            def_col_width=25,
            select_mode='browse',
            num_rows=15,
            justification='left',
            enable_events=True,
            key='_TREE_RESULTS_MAIN_'
        )
        coltable_results_main = sg.Column([[tree_results_main]],
                                          size=(575, 300),
                                          scrollable=True
                                          )

        col_table_results_main = sg.Column(
            [
                [title_downloadstudies],
                [text_table_results_main, btn_load_find_results_main, text_descriptor_main], #btn_sort_main],
                [coltable_results_main],
            ],
            visible=False,
            key=self.DISPLAY_RESULTS
        )

        # ------ BUTTONS, INPUTS, and OPTIONS: Load, Find, Move

        # ---
        btn_loadnew_main = sg.Button('New Project',
                                     size=(18, 1),
                                     button_color=self.default_btn_color,
                                     key=self.BUTTON_NEW)
        str_loadnew = 'Choose this option if you have a completely new set of studies to download.'
        txt_loadnew_main = sg.Text('\n'.join(textwrap.wrap(str_loadnew, 90)))
        btn_loadold_main = sg.Button('Existing Project',
                                          size=(18, 1),
                                          button_color=self.default_btn_color,
                                          key=self.BUTTON_OLD)
        str_loadold = 'Choose this option if you are returning to an existing project (typically with an ' \
                      'updated/addended query file). You will need to specify the previous project directory that ' \
                      'you downloaded files to. In particular, the SNAPSHOT file in that directory will be ' \
                      'used for validation purposes. This option is useful for preserving continuity in the ' \
                      'automatic numbering of queries, as well as tracking which studies have or have not been ' \
                      'downloaded.'
        txt_loadold_main = sg.Text('\n'.join(textwrap.wrap(str_loadold, 90)))
        col_loadchoice_main = sg.Column(
            [
                [self.spacer(sz=(1, 1))],
                [btn_loadnew_main],
                [txt_loadnew_main],
                [self.spacer(sz=(1, 1))],
                [btn_loadold_main],
                [txt_loadold_main]
            ],
            key=self.DISPLAY_CHOICE
        )


        # --- load parameters
        self.str_loadparam_new = 'LOAD PARAMETERS: New Project'
        self.str_loadparam_old = 'LOAD PARAMETERS: Existing Project'
        title_loadparam = sg.Text(self.str_loadparam_new, text_color='darkblue', font='Any 18 bold',size=(30, 1),
                                      visible=True,
                                      key=self.DISPLAY_TITLE_LOAD)

        label_loadqueryfile = sg.Text('Query File (Studies)', text_color='darkblue', font='Any 11')
        txt_loadqueryfile = sg.Input('', size=(80, 1), key='_TXT_LOADQUERIES_')
        btn_loadqueryfile = sg.Button('Browse', key=self.BUTTON_QUERYFILE)

        self.str_loadqueryfile_new = 'Select an XLSX file that contains identifying information on studies of ' \
                                    'interest. ' \
                                'For a NEW project, see "./xlsx/_TEMPLATE_.xlsx" for a sample input file. The single ' \
                                     'best ' \
                                'study identifier is accession number. Failing that, a combination ' \
                                'of MRN, study description, and date usually suffice. You may optionally add a ' \
                                     'column titled "QueryNumber" to override the default query numbering.'
        self.str_loadqueryfile_new = '\n'.join(textwrap.wrap(self.str_loadqueryfile_new, 90))
        self.str_loadqueryfile_old = 'Select an XLSX file that contains identifying information on studies of ' \
                                    'interest. For an EXISTING project, the new query file will be crosschecked against old queries and ' \
                                    'results from the previously created "SNAPSHOT" file in the project directory. ' \
                                    'Any new ' \
                                    'queries will be incorporated.'
        self.str_loadqueryfile_old = '\n'.join(textwrap.wrap(self.str_loadqueryfile_old, 90))
        txt_instructions_loadqueryfile = sg.Text(self.str_loadqueryfile_new, size=(70, 5),
                                                 key=self.DISPLAY_TXT_QUERYFILE)

        label_loaddir = sg.Text('Project Storage Directory', text_color='darkblue', font='Any 11')
        txt_loaddir = sg.Input('', size=(80, 1), key='_TXT_STORAGEDIR_')
        btn_loaddir = sg.Button('Browse', key=self.BUTTON_STORAGEDIR)

        self.str_loaddir_new = 'Select a directory to store downloaded studies for this project. *For a NEW project: ' \
                               'By default, the output directory is autogenerated based on the name of the input ' \
                               'query file.'
        self.str_loaddir_new = '\n'.join(textwrap.wrap(self.str_loaddir_new, 90))
        self.str_loaddir_old = 'Select a directory to store downloaded studies for this project. For an EXISTING ' \
                               'project, ensure that the project directory has all 3 "SNAPSHOT" files.'
        self.str_loaddir_old = '\n'.join(textwrap.wrap(self.str_loaddir_old, 90))
        txt_instructions_loaddir = sg.Text(self.str_loaddir_new, size=(70, 5),
                                           key=self.DISPLAY_TXT_LOADDIR)

        btn_parsequeries = sg.Button('Load Queries', key=self.BUTTON_LOAD_QUERIES)

        col_loadparameters_main = sg.Column(
            [
                [title_loadparam],
                # Queries file
                [label_loadqueryfile, btn_loadqueryfile],
                [txt_loadqueryfile],
                [txt_instructions_loadqueryfile],
                [self.spacer()],
                # Storage Directory
                [label_loaddir, btn_loaddir],
                [txt_loaddir],
                [txt_instructions_loaddir],
                [self.spacer()],
                [btn_parsequeries]
            ],
            visible=False,
            key=self.DISPLAY_PARAMETERS
        )

        btn_find_main = sg.Button('Query Database',
                                  size=(20, 1),
                                  button_color=self.default_btn_color,
                                  key=self.BUTTON_FIND)
        btn_move_main = sg.Button('Transfer Studies',
                                  size=(20, 1),
                                  button_color=self.default_btn_color,
                                  key=self.BUTTON_MOVE)
        checkbox_exactstudy_MAIN = sg.Checkbox('Exact Study Description', size=(20, 1), default=False,
                                               key='_EXACT_MATCH_STUDYDESCRIPTION_')
        checkbox_exactseries_MAIN = sg.Checkbox('Exact Series Description', size=(20, 1), default=False,
                                                key='_EXACT_MATCH_SERIESDESCRIPTION_')
        checkbox_skip_MAIN = sg.Checkbox('Skip Existing Studies', default=False, size=(17, 1), disabled=True,
                                         key='_SKIP_MAIN_')
        checkbox_anonymize_MAIN = sg.Checkbox('Anonymize Studies', size=(17, 1), default=True,
                                              key='_ANONYMIZE_MAIN_')
        """
        col_checkbox_MAIN = sg.Column([
            [self.spacer()],
            [checkbox_skip_MAIN],
            [checkbox_anonymize_MAIN]
        ])
        """

        # ------ COMBO: Source Peer
        text_src_main = sg.Text('Database', text_color='darkblue', font='Any 11')
        combo_src_main = sg.Combo([], size=(21, 1), readonly=True, key='_COMBO_SRC_MAIN_')
        col_src_main = sg.Column(
            [
                [text_src_main],
                [checkbox_exactstudy_MAIN, combo_src_main],
                [checkbox_exactseries_MAIN, btn_find_main]
            ],
            visible=False,
            key=self.DISPLAY_FIND
        )

        col_arrow_main_fromsearchtofilter = sg.Column([
            [self.spacer()],
            [sg.Text('-'*34 + '>', font='Any 11')],  #, text_color='blue')]
        ], pad=(0, 0))

        col_arrow_main_fromfiltertotransfer = sg.Column([
            [self.spacer()],
            [sg.Text('-' * 34 + '>', font='Any 11')],  # , text_color='blue')]
        ], pad=(0, 0))

        # ------ COMBO: Destination Peer
        text_dest_main = sg.Text('Destination (Peer)', text_color='darkblue', font='Any 11')
        combo_dest_main = sg.Combo([], size=(21, 1), readonly=True, key='_COMBO_DEST_MAIN_')
        col_dest_main = sg.Column(
            [
                [text_dest_main],
                [checkbox_skip_MAIN, combo_dest_main],
                [checkbox_anonymize_MAIN, btn_move_main]
            ],
            visible=False,
            key=self.DISPLAY_MOVE
        )

        # ------ C-MOVE STUDY/SERIES SELECTION
        text_dest_btns_main = sg.Text('Selection Filters', text_color='darkblue', font='Any 11')
        combo_headings_simple_main = sg.Combo(headings_pretty_results_main,
                                       default_value=default_filter_main,
                                       size=(21, 1),
                                       readonly=True,
                                       key='_COMBO_FILT_SIMPLE_MAIN_')
        btn_value_selector_simple = sg.Button('SELECT VALUES',
                                           size=(20, 1),
                                           button_color=self.default_btn_color,
                                           disabled=True,
                                           key='_FILT_SIMPLE_MAIN_')
        btn_value_selector = sg.Button('Confirm Selected Series',
                                       size=(24, 1),
                                       button_color=self.default_btn_color,
                                       key=self.BUTTON_FILTER)

        col_selection_btn_main = sg.Column(
            [
                [text_dest_btns_main],
                [btn_value_selector]
            ],
            visible=False,
            key=self.DISPLAY_FILTER
        )

        # ------ Navigation buttons
        btn_back = sg.Button('Back',
                             size=(10, 1),
                             button_color=self.default_btn_color,
                             disabled=True,
                             key=self.BUTTON_BACK)
        btn_next = sg.Button('Next',
                             size=(10, 1),
                             button_color=self.default_btn_color,
                             disabled=True,
                             key=self.BUTTON_NEXT)
        col_navigation = sg.Column(
            [
                [btn_back, btn_next]
            ],
            key=self.DISPLAY_NAVIGATION
        )


        # ------ ASSEMBLE:
        col_tab_main = sg.Column(
            [
                [col_loadchoice_main, col_loadparameters_main, col_table_raw_main, col_table_results_main],
                [col_src_main, col_selection_btn_main, col_dest_main],
            ]
        )
        tablayout_main = [
            [col_tab_main, self.spacer(sz=(0, 32))],
            [col_navigation]
        ]
        tab_main = sg.Tab('Main', tablayout_main)

        # -------------------------- TAB: Config/Settings

        # - local server storage directory
        text_label_localdir_cfg = sg.Text('Directory: ')  # , size=(13, 1))
        text_dir_localdir_cfg = sg.Text('', size=(56, 1), key='_DIR_LOCAL_CFG_')
        btn_browse_localdir_cfg = sg.Button('Browse', key='_BTN_LOCAL_CFG_')

        framelayout_localdir_cfg = [
            [text_label_localdir_cfg, text_dir_localdir_cfg, btn_browse_localdir_cfg]
        ]
        frame_localdir_cfg = sg.Frame('Local File Storage Location', framelayout_localdir_cfg, font='Any 11')
        col_localdir_cfg = sg.Column([
            [frame_localdir_cfg]
        ])

        # -- Peers Config

        text_currentpeer_cfg = sg.Text('Peer Name', text_color='darkblue', font='Any 11')
        input_name_peers_cfg = sg.Input('', do_not_clear=True, size=(25, 1), font=None, key='_NAME_PEER_CFG_')
        text_savedpeers_cfg = sg.Text('Saved Peers', text_color='darkblue', font='Any 11')
        lst_peers_cfg = sg.Listbox(
            values=[],
            size=(25, 5),
            select_mode='none',
            key='_LST_PEERS_CFG_')

        col1_peers_cfg = sg.Column([
            [text_currentpeer_cfg],
            [input_name_peers_cfg],
            [text_savedpeers_cfg],
            [lst_peers_cfg]
        ])
        # - peer details
        text_details_peer_cfg = sg.Text('Peer Details', text_color='darkblue', font='Any 11')
        # - peer AET
        text_aet_peer_cfg = sg.Text('AET:', size=(5, 1))
        input_aet_peer_cfg = sg.Input('', do_not_clear=True, size=(18, 1), key='_AET_PEER_CFG_')
        # - peer port
        text_port_peer_cfg = sg.Text('Port:', size=(5, 1))
        input_port_peer_cfg = sg.Input('', do_not_clear=True, size=(18, 1), key='_PORT_PEER_CFG_')
        # - peer IP
        text_ip_peer_cfg = sg.Text('IP:', size=(5, 1))
        input_ip_peer_cfg = sg.Input('', do_not_clear=True, size=(18, 1), key='_IP_PEER_CFG_')

        btn_load_peer_cfg = sg.Button('Load', size=(6, 1), key='_BTN_LOAD_PEER_CFG_')
        btn_save_peer_cfg = sg.Button('Save', size=(6, 1), key='_BTN_SAVE_PEER_CFG_')
        btn_delete_peer_cfg = sg.Button('Delete', size=(6, 1), key='_BTN_DELETE_PEER_CFG_')

        col2_peers_cfg = sg.Column([
            [text_details_peer_cfg],
            [text_aet_peer_cfg, input_aet_peer_cfg],
            [text_ip_peer_cfg, input_ip_peer_cfg],
            [text_port_peer_cfg, input_port_peer_cfg],
            [self.spacer()],
            [btn_load_peer_cfg, btn_save_peer_cfg, btn_delete_peer_cfg]
        ])



        # -- Assemble Peers Config
        framelayout_peers_cfg = [
            [col1_peers_cfg, col2_peers_cfg],
        ]
        frame_peers_cfg = sg.Frame('Peers', framelayout_peers_cfg, font='Any 11')

        col_settings_cfg = sg.Column([
            [frame_peers_cfg]
        ])

        # --- Assemble config tab
        tablayout_cfg = [
            [col_localdir_cfg],
            [col_settings_cfg],
        ]
        tab_cfg = sg.Tab('Settings', tablayout_cfg)

        # --- Tabgroup
        tabgroup = sg.TabGroup(
            [
                [tab_main, tab_cfg]
            ]
        )

        # --- File Menu
        menu_def = [
            ['!File',
             ['!Load New Peer Configs', '!Load Additional Peer Configs', '!Save Peer Configs']
             ],
            ['!Edit',
             ['!Paste',
              ['!Special', '!Normal'],
              '!Undo'
              ],
             ],
            ['!Help',
             '!About...'
             ],
        ]
        menu = sg.Menu(menu_def, key='_MENU_')

        # *******************************
        # Final Layout
        # *******************************

        layout = [
            [menu],
            [tabgroup],
        ]

        return sg.Window('Medical Imaging Downloader', layout).Finalize()


    def createSelectorLayout(self):
        # --- selector window elements
        # ------ Selector Pop Up
        # Given a list, will allow user to choose which elements to select and in what order.

        # --- Available options column
        txt_all_SEL = sg.Text('Available', size=(26, 1), text_color='darkblue', font='Any 11', key='_TXT_ALL_SEL_')
        lst_all_SEL = sg.Listbox(
            values=[],
            size=(36, 17),
            select_mode='extended',
            key='_LST_ALL_SEL_')
        col_all_SEL = sg.Column([
            [txt_all_SEL],
            [lst_all_SEL]
        ],
        background_color='salmon')

        # --- Buttons column
        btn_add_SEL = sg.Button('Add -->', size=(12, 1), key='_BTN_ADD_SEL_')
        btn_remove_SEL = sg.Button('<-- Remove', size=(12, 1), key='_BTN_REMOVE_SEL_')
        btn_addall_SEL = sg.Button('Add All -->', size=(12, 1), key='_BTN_ADDALL_SEL_')
        btn_removeall_SEL = sg.Button('<-- Remove All', size=(12, 1), key='_BTN_REMOVEALL_SEL_')
        btn_restore_SEL = sg.Button('Restore Original', size=(12, 1), key='_BTN_RESTORE_SEL_')

        col_buttons_SEL = sg.Column([
            [self.spacer()],
            [self.spacer()],
            [self.spacer()],
            [btn_add_SEL],
            [btn_remove_SEL],
            [self.spacer()],
            [btn_addall_SEL],
            [btn_removeall_SEL],
            [btn_restore_SEL],
        ])

        # --- Selected options column
        txt_selected_SEL = sg.Text('Selected', size=(26, 1), text_color='darkblue', font='Any 11', key='_TXT_SEL_SEL_')
        lst_selected_SEL = sg.Listbox(
            values=[],
            size=(36, 17),
            select_mode='extended',
            key='_LST_SEL_SEL_')
        btn_up_selected_SEL = sg.Button('Move up', key='_BTN_UP_SEL_')
        btn_down_selected_SEL = sg.Button('Move down', key='_BTN_DOWN_SEL_')
        col_selected_SEL = sg.Column([
            [txt_selected_SEL],
            [lst_selected_SEL],
            [btn_up_selected_SEL, btn_down_selected_SEL]
        ],
        background_color='green1')

        # Ok/Cancel for selector window
        btn_ok_SEL = sg.Button('OK', size=(8, 2), key='_BTN_OK_SEL_')
        btn_cancel_SEL = sg.Button('Cancel', size=(8, 2), key='_BTN_CANCEL_SEL_')

        selector_layout = [
            [col_all_SEL, col_buttons_SEL, col_selected_SEL],
            [btn_ok_SEL, btn_cancel_SEL]
        ]

        return selector_layout

    def createAdvancedSelectorLayout(self):

        # --------- INDEPENDENT header
        txt_first_SEL = sg.Text('Independent Variable', size=(26, 1), text_color='darkblue', font='Any 11',
                                 key='_TXT_FIRST_SEL_')
        combo_first_SEL = sg.Combo([],
                                    size=(21, 1),
                                    readonly=True,
                                    enable_events=True,
                                    key='_COMBO_FIRST_SEL_')
        txt_val_first_SEL = sg.Text('All', size=(26, 1), text_color='darkblue', font='Any 11',
                                    key='_TXT_VAL_FIRST_SEL_')
        lst_first_SEL = sg.Listbox(
            values=[],
            size=(36, 17),
            select_mode='single',
            enable_events=True,
            key='_LST_FIRST_SEL_')
        frame_first_SEL = sg.Frame('', [
            [txt_val_first_SEL],
            [lst_first_SEL]
        ])

        col_first_SEL = sg.Column([
            [txt_first_SEL],
            [combo_first_SEL],
            [frame_first_SEL]
        ])


        # --------- DEPENDENT header (that depends on the first)
        txt_second_SEL = sg.Text('Dependent Variable', size=(26, 1), text_color='darkblue', font='Any 11',
                                 key='_TXT_SECOND_SEL_')
        combo_second_SEL = sg.Combo([],
                                    size=(21, 1),
                                    readonly=True,
                                    enable_events=True,
                                    key='_COMBO_SECOND_SEL_')

        selector_layout = self.createSelectorLayout()
        frame_second_SEL = sg.Frame('', selector_layout)

        col_second_SEL = sg.Column([
            [txt_second_SEL],
            [combo_second_SEL],
            [frame_second_SEL]
        ])


        # --------- Assemble the advanced selector layout
        advanced_selector_layout = [
            [col_first_SEL, sg.VerticalSeparator(), col_second_SEL]
        ]
        return advanced_selector_layout


    def createUI(self,
                 query_headings,
                 results_headings,
                 padding_pretty_raw_main=None,
                 padding_pretty_results_main=None,
                 default_filter_main=None):


        # *******************************
        # MAIN WINDOW
        # *******************************
        self.main_window = self.createMainWindow(query_headings, results_headings,
                                                 padding_pretty_raw_main=padding_pretty_raw_main,
                                                 padding_pretty_results_main=padding_pretty_results_main,
                                                 default_filter_main=default_filter_main)

        self.set_phase(Phase.PHASE_CHOICE, Phase.PHASE_CHOICE)

        # *******************************
        # THEMES (they're all ugly)
        # *******************************
        sg.SetOptions(element_padding=(5, 5))
        sg.ChangeLookAndFeel('SystemDefault')
        # Save the default button colors, so the we can use them during the phases

        """
        SystemDefault
        Reddit
        Topanga
        GreenTan
        Dark
        LightGreen
        Dark2
        Black
        Tan
        TanBlue
        DarkTanBlue
        DarkAmber
        DarkBlue
        Reds
        Green
        BluePurple
        Purple
        BlueMono
        GreenMono
        BrownBlue
        BrightColors
        NeutralBlue
        Kayak
        SandyBeach
        TealMono
        """



