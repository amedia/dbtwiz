from ..config.user import user_config


def custom_style():
    """Pick a suitable Style for questionary based on user's theme"""
    from questionary import Style  # Lazy import for improved performance

    if user_config().theme == "dark":
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
        # Light theme with colors adjusted for bright background
        style_dict = dict(
            qmark="fg:#0066cc bold",  # token in front of the question (blue)
            question="bold",  # question text
            answer="fg:#cc6600 bg:#f0f0f0 bold",  # submitted answer text (orange with light gray background)
            pointer="fg:#0066cc bold",  # pointer used in select and checkbox prompts (blue)
            highlighted="fg:#cc6600 bold",  # pointed-at choice in select and checkbox prompts (orange)
            selected="fg:#ffffff bg:#0066cc",  # style for a selected item (white text on blue background)
            separator="fg:#cc0000",  # separator in lists (red)
            instruction="fg:#666666",  # user instructions (gray)
            text="",  # plain text
            disabled="fg:#999999 italic",  # disabled choices (light gray)
        )
    return Style.from_dict(style_dict)
