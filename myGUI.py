import sys, os
if sys.version_info[0] >= 3:
    import PySimpleGUI as sg
else:
    import PySimpleGUI27 as sg

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
    # PHASES
    # *******************************
    # phase names
    PHASE_LOAD = 0
    PHASE_FIND = 1
    PHASE_FILT = 2
    PHASE_MOVE = 3
    PHASE_DONE = 4
    PHASE_LOCK = 5

    # default colors
    default_btn_back = 'darkblue'
    default_btn_text = 'white'
    default_btn_color = (default_btn_text, default_btn_back)
    highlight_btn_back = 'green1'
    highlight_btn_text = 'black'
    highlight_btn_color = (highlight_btn_text, highlight_btn_back)

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

    def set_tree(self, window, key, studyNodes):
        tree_element = window.Element(key)

        # --- create UI tree
        treedata = sg.TreeDict()

        # track unique keys, since we may encounter multiple duplicate series/study descriptions
        unique_keys = {}

        # studyNodes is a list of unique combinations of one study description and its associated series descriptions
        for studyNode in studyNodes:

            key = studyNode.studyDescription
            if key in unique_keys:
                unique_keys[key] += 1
            else:
                unique_keys[key] = 1
            unique_key = '_' + key + str(unique_keys[key]) + '_'

            treedata.Insert(
                parent='',
                key=unique_key,
                text=studyNode.studyDescription,
                values=[studyNode.count],
                icon=(sg.PSG_DEBUGGER_LOGO if studyNode.is_selected else sg.PSG_DEBUGGER_LOGO)
            )
            # for each unique series description in each unique study description
            for series_description in sorted(studyNode.series_nodes.keys()):
                series_is_selected = studyNode.series_nodes[series_description]

                if series_description in unique_keys:
                    unique_keys[series_description] += 1
                else:
                    unique_keys[series_description] = 1
                series_unique_key = '_' + series_description + str(unique_keys[series_description]) + '_'

                treedata.Insert(
                    parent=unique_key,
                    key=series_unique_key,
                    text=series_description,
                    icon=(sg.PSG_DEBUGGER_LOGO if series_is_selected else sg.PSG_DEBUGGER_LOGO)
                )

        tree_element.Update(treedata)


    def enableButton(self, window, key, enabled):
        disabled = not enabled
        window.Element(key).Update(disabled=disabled)

    def colorButton(self, window, key, color=(default_btn_text, default_btn_back)):
        # color is a tuple: (text_color, button_color) ('white' on 'darkblue')
        window.Element(key).Update(button_color=color)

    def set_phase_LOAD(self):
        # enable/disable
        self.enableButton(self.main_window, '_LOAD_MAIN_', True)
        self.enableButton(self.main_window, '_LOAD_SEARCH_RESULTS_MAIN_', True)
        self.enableButton(self.main_window, '_FIND_MAIN_', False)
        self.enableButton(self.main_window, '_FILT_MAIN_', False)
        self.enableButton(self.main_window, '_MOVE_MAIN_', False)
        # color
        self.colorButton(self.main_window, '_LOAD_MAIN_', self.highlight_btn_color)
        self.colorButton(self.main_window, '_FIND_MAIN_')
        self.colorButton(self.main_window, '_FILT_MAIN_')
        self.colorButton(self.main_window, '_MOVE_MAIN_')

    def set_phase_FIND(self):
        # enable/disable
        self.enableButton(self.main_window, '_LOAD_MAIN_', True)
        self.enableButton(self.main_window, '_LOAD_SEARCH_RESULTS_MAIN_', True)
        self.enableButton(self.main_window, '_FIND_MAIN_', True)
        self.enableButton(self.main_window, '_FILT_MAIN_', False)
        self.enableButton(self.main_window, '_MOVE_MAIN_', False)
        # color
        self.colorButton(self.main_window, '_LOAD_MAIN_')
        self.colorButton(self.main_window, '_FIND_MAIN_', self.highlight_btn_color)
        self.colorButton(self.main_window, '_FILT_MAIN_')
        self.colorButton(self.main_window, '_MOVE_MAIN_')
    def set_phase_FILT(self):
        # enable/disable
        self.enableButton(self.main_window, '_LOAD_MAIN_', True)
        self.enableButton(self.main_window, '_LOAD_SEARCH_RESULTS_MAIN_', True)
        self.enableButton(self.main_window, '_FIND_MAIN_', True)
        self.enableButton(self.main_window, '_FILT_MAIN_', True)
        self.enableButton(self.main_window, '_MOVE_MAIN_', False)
        # color
        self.colorButton(self.main_window, '_LOAD_MAIN_')
        self.colorButton(self.main_window, '_FIND_MAIN_')
        self.colorButton(self.main_window, '_FILT_MAIN_', self.highlight_btn_color)
        self.colorButton(self.main_window, '_MOVE_MAIN_')
    def set_phase_MOVE(self):
        # enable/disable
        self.enableButton(self.main_window, '_LOAD_MAIN_', True)
        self.enableButton(self.main_window, '_LOAD_SEARCH_RESULTS_MAIN_', True)
        self.enableButton(self.main_window, '_FIND_MAIN_', True)
        self.enableButton(self.main_window, '_FILT_MAIN_', True)
        self.enableButton(self.main_window, '_MOVE_MAIN_', True)
        # color
        self.colorButton(self.main_window, '_LOAD_MAIN_')
        self.colorButton(self.main_window, '_FIND_MAIN_')
        self.colorButton(self.main_window, '_FILT_MAIN_')
        self.colorButton(self.main_window, '_MOVE_MAIN_', self.highlight_btn_color)
    def set_phase_DONE(self):
        # enable/disable
        self.enableButton(self.main_window, '_LOAD_MAIN_', True)
        self.enableButton(self.main_window, '_LOAD_SEARCH_RESULTS_MAIN_', True)
        self.enableButton(self.main_window, '_FIND_MAIN_', True)
        self.enableButton(self.main_window, '_FILT_MAIN_', True)
        self.enableButton(self.main_window, '_MOVE_MAIN_', True)
        # color
        self.colorButton(self.main_window, '_LOAD_MAIN_')
        self.colorButton(self.main_window, '_FIND_MAIN_')
        self.colorButton(self.main_window, '_FILT_MAIN_')
        self.colorButton(self.main_window, '_MOVE_MAIN_')
    def set_phase_LOCK(self):
        # enable/disable
        self.enableButton(self.main_window, '_LOAD_MAIN_', False)
        self.enableButton(self.main_window, '_LOAD_SEARCH_RESULTS_MAIN_', False)
        self.enableButton(self.main_window, '_FIND_MAIN_', False)
        self.enableButton(self.main_window, '_FILT_MAIN_', False)
        self.enableButton(self.main_window, '_MOVE_MAIN_', False)

    def set_phase(self, phase):
        switcher = {
            self.PHASE_LOAD: self.set_phase_LOAD,
            self.PHASE_FIND: self.set_phase_FIND,
            self.PHASE_FILT: self.set_phase_FILT,
            self.PHASE_MOVE: self.set_phase_MOVE,
            self.PHASE_DONE: self.set_phase_DONE,
            self.PHASE_LOCK: self.set_phase_LOCK
        }
        switcher[phase]()

    # *******************************
    # POPUP FUNCTIONS
    # *******************************

    def popup(self, txt, title=None, keep_on_top=True):
        sg.Popup(txt, title=title, keep_on_top=keep_on_top)

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
        text_table_raw_main = sg.Text('    Studies of Interest (Queries)', text_color='darkblue', font='Any 11')
        txt_load_main = sg.Input('', visible=False, do_not_clear=False, enable_events=True, key='_TXT_LOAD_MAIN_')
        btn_load_main = sg.Button('1. Load',
                                  size=(7, 1),
                                  button_color=self.default_btn_color,
                                  key='_LOAD_MAIN_')
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
                                      size=(300, 300),
                                      scrollable=True
                                      )
        col_table_raw_main = sg.Column([
            [text_table_raw_main, btn_load_main, txt_load_main],
            [coltable_raw_main]
        ])

        # ------ TABLE: query results
        text_table_results_main = sg.Text('    Query Results', size=(17, 1), text_color='darkblue', font='Any 11',
                                          key='_LABEL_RESULTS_MAIN_')
        btn_sort_main = sg.Button('Sort',
                                  size=(9, 1),
                                  key='_SORT_MAIN_')
        btn_load_find_results_main = sg.Button('Load',
                                  size=(6, 1),
                                  button_color=self.default_btn_color,
                                  key='_LOAD_SEARCH_RESULTS_MAIN_')
        text_descriptor_main = sg.Text('', size=(85, 1), key='_DESCRIPTOR_MAIN_')

        results_padding = padding_pretty_results_main if padding_pretty_results_main else [''] * len(
            headings_pretty_results_main)
        table_results_main = sg.Table(
            values=[results_padding],
            headings=headings_pretty_results_main,
            select_mode='extended',
            num_rows=15,
            alternating_row_color='#ffffff',
            vertical_scroll_only=True,
            hide_vertical_scroll=True,
            justification='left',
            key='_TABLE_RESULTS_MAIN_'
        )
        coltable_results_main = sg.Column([[table_results_main]],
                                          size=(900, 300),
                                          scrollable=True
                                          )

        col_table_results_main = sg.Column([
            [text_table_results_main, btn_load_find_results_main, btn_sort_main, text_descriptor_main],
            [coltable_results_main],
        ])

        # ------ BUTTONS and OPTIONS: Find, Move
        btn_find_main = sg.Button('2. SEARCH DATABASE',
                                  size=(20, 1),
                                  button_color=self.default_btn_color,
                                  key='_FIND_MAIN_')
        btn_move_main = sg.Button('4. TRANSFER STUDIES',
                                  size=(20, 1),
                                  button_color=self.default_btn_color,
                                  key='_MOVE_MAIN_')
        checkbox_exactstudy_MAIN = sg.Checkbox('Exact Study Description', default=False,
                                               key='_EXACT_MATCH_STUDYDESCRIPTION_')
        checkbox_exactseries_MAIN = sg.Checkbox('Exact Series Description', default=False,
                                                key='_EXACT_MATCH_SERIESDESCRIPTION_')
        checkbox_skip_MAIN = sg.Checkbox('Skip Existing Studies', default=False, disabled=True, key='_SKIP_MAIN_')
        checkbox_anonymize_MAIN = sg.Checkbox('Anonymize', default=True, key='_ANONYMIZE_MAIN_')
        """
        col_checkbox_MAIN = sg.Column([
            [self.spacer()],
            [checkbox_skip_MAIN],
            [checkbox_anonymize_MAIN]
        ])
        """

        # ------ COMBO: Source Peer
        text_src_main = sg.Text('Source (Peer)', text_color='darkblue', font='Any 11')
        combo_src_main = sg.Combo([], size=(21, 1), readonly=True, key='_COMBO_SRC_MAIN_')
        col_src_main = sg.Column([
            [text_src_main],
            [combo_src_main],
            [checkbox_exactstudy_MAIN],
            [checkbox_exactseries_MAIN],
            [btn_find_main],
        ])

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
        col_dest_main = sg.Column([
            [text_dest_main],
            [combo_dest_main],
            [checkbox_skip_MAIN],
            [checkbox_anonymize_MAIN],
            [btn_move_main],
        ])

        # ------ C-MOVE STUDY/SERIES SELECTION
        text_dest_btns_main = sg.Text('Selection Filters', text_color='darkblue', font='Any 11')
        combo_headings_simple_main = sg.Combo(headings_pretty_results_main,
                                       default_value=default_filter_main,
                                       size=(21, 1),
                                       readonly=True,
                                       key='_COMBO_FILT_SIMPLE_MAIN_')
        btn_value_selector_simple = sg.Button('3. SELECT VALUES',
                                           size=(20, 1),
                                           button_color=self.default_btn_color,
                                           disabled=True,
                                           key='_FILT_SIMPLE_MAIN_')
        btn_value_selector = sg.Button('3. SELECT STUDIES/SERIES',
                                       size=(24, 1),
                                       button_color=self.default_btn_color,
                                       disabled=True,
                                       key='_FILT_MAIN_')

        col_selection_btn_main = sg.Column([
            [text_dest_btns_main],
            [btn_value_selector]
        ])

        # - local server storage directory
        text_label_localdir_main = sg.Text('Directory: ')  # , size=(13, 1))
        text_dir_localdir_main = sg.Text('', size=(56, 1), key='_DIR_LOCAL_CFG_')
        btn_browse_localdir_main = sg.Button('Browse', key='_BTN_LOCAL_CFG_')

        framelayout_localdir_main = [
            [text_label_localdir_main, text_dir_localdir_main, btn_browse_localdir_main]
        ]
        frame_localdir_main = sg.Frame('Local Server', framelayout_localdir_main, font='Any 11')
        col_middle_main = sg.Column([
            [col_arrow_main_fromsearchtofilter, col_selection_btn_main, col_arrow_main_fromfiltertotransfer],
            [frame_localdir_main]
        ])

        # ------ ASSEMBLE:
        tablayout_main = [
            [col_table_raw_main, col_table_results_main],
            [col_src_main, col_middle_main, col_dest_main]
        ]
        tab_main = sg.Tab('Main', tablayout_main)

        # -------------------------- TAB: Config/Settings

        # -- Peers Config

        text_currentpeer_cfg = sg.Text('Peer Name', text_color='darkblue', font='Any 11')
        input_name_peers_cfg = sg.Input('', do_not_clear=True, size=(25, 1), font=None, key='_NAME_PEER_CFG_')
        text_savedpeers_cfg = sg.Text('Saved Peers', text_color='darkblue', font='Any 11')
        lst_peers_cfg = sg.Listbox(
            values=[],
            size=(25, 5),
            select_mode='single',
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
            #[frame_local_cfg],
            [frame_peers_cfg]
        ])

        # --- STDOUT output, for debugging purposes
        #output_cfg = sg.Output(size=(75, 20))

        # --- Assemble config tab
        tablayout_cfg = [
            [col_settings_cfg, ]#output_cfg]
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



