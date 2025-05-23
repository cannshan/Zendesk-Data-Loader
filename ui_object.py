from tkinter import Label, Listbox, StringVar, END

class UIObject:
    def __init__(self, parent, callback):
        self.parent = parent
        self.callback = callback
        self.selected_object = StringVar()
        self.selected_object.set("organizations")  # Default selection

        Label(parent, text="Select an object:").pack()

        self.objects = ['organizations', 'users', 'tickets']

        self.listbox = Listbox(parent, selectmode="single", height=len(self.objects))
        for obj in self.objects:
            self.listbox.insert(END, obj)

        self.listbox.pack()
        self.listbox.bind("<<ListboxSelect>>", self.on_select)

    def on_select(self, event):
        try:
            selection = self.listbox.curselection()
            if selection:
                self.selected_object.set(self.objects[selection[0]])
                self.callback(self.selected_object.get())
        except IndexError:
            pass
