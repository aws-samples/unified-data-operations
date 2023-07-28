from prompt_toolkit.key_binding import KeyBindings, KeyPressEvent

kb = KeyBindings()
@kb.add("c-space")
def _(event):
    "Initialize autocompletion, or select the next completion."
    buff = event.app.current_buffer
    if buff.complete_state:
        buff.complete_next()
    else:
        buff.start_completion(select_first=False)

