from tkinter import Label, Listbox, Scrollbar, StringVar, END

class UIAction:
    def __init__(self, parent, callback):
        self.parent = parent
        self.callback = callback
        self.selected_action = StringVar()
        self.selected_action.set("Import")  # Default selection

        Label(parent, text="Select an action:").pack()

        self.actions = ['Import', 'Update', 'Delete', 'Export']

        frame = parent  # Using parent as the frame directly
        scrollbar = Scrollbar(frame, orient="vertical")

        self.listbox = Listbox(frame, selectmode="single", height=len(self.actions), yscrollcommand=scrollbar.set)
        for action in self.actions:
            self.listbox.insert(END, action)

        self.listbox.pack(side="left", fill="y")
        scrollbar.config(command=self.listbox.yview)
        scrollbar.pack(side="right", fill="y")

        self.listbox.bind("<<ListboxSelect>>", self.on_select)

    def on_select(self, event):
        try:
            selection = self.listbox.curselection()
            if selection:
                self.selected_action.set(self.actions[selection[0]])
                self.callback(self.selected_action.get())
        except IndexError:
            pass


def disable_all_buttons_except_stop():
    # JavaScript function call to disable all buttons except the Stop button
    webview.evaluate_js("document.querySelectorAll('button:not(#stopButton)').forEach(btn => btn.disabled = true);")

def disable_all_buttons():
    # JavaScript function call to disable all buttons including the Stop button
    webview.evaluate_js("document.querySelectorAll('button').forEach(btn => btn.disabled = true);")

def enable_all_buttons():
    # JavaScript function call to re-enable all buttons
    webview.evaluate_js("document.querySelectorAll('button').forEach(btn => btn.disabled = false);")
