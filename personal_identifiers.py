import re
import emoji


def distinct_emoji_list(string):
    """Resturns distinct list of emojis from the string"""
    return {x['emoji'] for x in emoji.emoji_list(string)}


def clean_personal_marker(phrase):
    """ Clean a clause extracted from a description"""
    if not phrase:
        return None

    # drop weird special characters
    phrase = phrase.encode('ascii', errors='ignore').decode().strip()
    x_prev = phrase

    while True:
        # remove excess whitespace
        phrase = re.sub(r"\s+", " ", phrase).strip()

        # address common cases
        phrase = re.sub(r"^i (love|like|enjoy) ", "", phrase)
        phrase = re.sub(r"^(i am|i'm|i'm) (a |an )?", "", phrase)
        phrase = re.sub(r"^(i |a[n]?)\b", "", phrase)
        phrase = re.sub(r"^(and|the|from|to)\b", "", phrase)
        phrase = re.sub(r" of$", "", phrase)
        phrase = re.sub(r'(on )?(snapchat|snap|ig|insta|instagram|email|phone): +[A-Za-z0-9_@.-]+', " ", phrase)
        phrase = re.sub(r'\u200d', "", phrase)

        phrase = phrase.replace("#", "")
        phrase = phrase.strip().strip(".,/!-]+[#@:)(-?'$%&_").strip()
        phrase = re.sub(r"[!\(\)?.\{\}]", " ", phrase).strip()
        if phrase == x_prev:
            return phrase

        x_prev = phrase


def generate_split_profile_description(description):
    """Splits up a profile description into a set of clauses. Returns the clauses and
    all emojis in the description (which are being treated as identity markers)
    """

    # remove URLs and email addresses
    d = re.sub(r'\w+@\w+\.\w+', '', description.lower()).strip()
    d = re.sub(r'http\S+', '', d).strip()
    d = d.replace("&emsp;", "").replace("&nbsp;", "")

    # get all emoji and remember them, then treat them as split characters
    emojis = distinct_emoji_list(d)
    d = get_emoji_regexp().sub("|", d)  # .encode("ascii","namereplace").decode()

    # split on sensible split characters
    # | and
    spl = [x for x in re.split(
        r"[\(\)|•*;~°,\n\t]|[!…]+|[-–\/.]+ | [&+:]+ | [+] |([\/])(?=[A-Za-z ])|([.!-]{2,})| and |([#@][A-Za-z0-9_]+)",
        d.lower()) if (
                   x and x.strip() != "" and not x.strip() in "|•&*#;~°.!…-/–")]

    # clean all clauses
    spl = [clean_personal_marker(x) for x in spl]
    # remove weird things and things that become empty
    spl = [x for x in spl if x.strip() != "" and x.encode() != b'\xef\xb8\x8f']
    return spl, emojis


def find_identifiers_simple(description):
    spl, emojis = generate_split_profile_description(description)
    return spl, emojis
def get_emoji_regexp():
    # Sort emoji by length to make sure multi-character emojis are
    # matched first
    emojis = sorted(emoji.EMOJI_DATA, key=len, reverse=True)
    pattern = u'(' + u'|'.join(re.escape(u) for u in emojis) + u')'
    return re.compile(pattern)
