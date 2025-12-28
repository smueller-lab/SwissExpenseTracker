from . import home, router, food, groceries

S_CALLBACKS = [home, router, food, groceries]

def register_all_callbacks(app, data):
    for module in S_CALLBACKS:
        module.register_callbacks(app, data)