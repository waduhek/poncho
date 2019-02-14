def reformat(text):
    '''Convert escape characters to strings and convert all double inverted commas to a single
    inverted comma.

    Args:
        text -> Text to be reformated.
    Returns:
        Reformatted text.
    '''
    return text.replace('\n', '<NL>').replace('\r', '<CR>').replace('"', "'").replace('\t', "<TAB>")
