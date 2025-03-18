from dbtwiz.config import UserConfig


def config(setting, value):
    """Updates user config."""
    if ":" in setting:
        section, key = setting.split(":")
    else:
        section, key = "general", setting
    UserConfig().update(section, key, value)
