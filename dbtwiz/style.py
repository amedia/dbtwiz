from dbtwiz.config import dark_mode


def custom_style():
    """Pick a suitable Style for questionary based on user's theme"""
    from questionary import Style  # Lazy import for improved performance

    if dark_mode():
        # style_dict = {
        #     'completion-menu.completion':         'bg:#333333 #ffffff',  # Completion items background a little lighter gray
        #     'completion-menu.completion.current': 'bg:#444444 #ffffff', # Selected completion item even lighter
        #     'completion-menu.completion.title':   '#cccccc',     # Completion item title
        #     'completion-menu.completion.meta':    '#888888',    # Completion item meta
        # }
        style_dict = dict(
            qmark="fg:#30d0d0 bold",  # token in front of the question
            question="bold",  # question text
            answer="fg:#d08030 bg:#333333 bold",  # submitted answer text behind the question
            pointer="fg:#30d0d0 bold",  # pointer used in select and checkbox prompts
            highlighted="fg:#d08030 bold",  # pointed-at choice in select and checkbox prompts
            selected="fg:#505050 bg:#ffffff",  # style for a selected item of a checkbox
            separator="fg:#c03030",  # separator in lists
            instruction="",  # user instructions for select, rawselect, checkbox
            text="",  # plain text
            disabled="fg:#858585 italic",  # disabled choices for select and checkbox prompts
        )
    else:
        # FIXME: Adjust to fit with bright background
        style_dict = dict(
            qmark="fg:#30d0d0 bold",  # token in front of the question
            question="bold",  # question text
            answer="fg:#d08030 bg:#333333 bold",  # submitted answer text behind the question
            pointer="fg:#30d0d0 bold",  # pointer used in select and checkbox prompts
            highlighted="fg:#d08030 bold",  # pointed-at choice in select and checkbox prompts
            selected="fg:#505050 bg:#ffffff",  # style for a selected item of a checkbox
            separator="fg:#c03030",  # separator in lists
            instruction="",  # user instructions for select, rawselect, checkbox
            text="",  # plain text
            disabled="fg:#858585 italic",  # disabled choices for select and checkbox prompts
        )
    return Style.from_dict(style_dict)
