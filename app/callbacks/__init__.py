from . import home, router, food, groceries, sport

S_CALLBACKS = [home, router, food, groceries, sport]

def register_all_callbacks(app, data):
    for module in S_CALLBACKS:
        module.register_callbacks(app, data)