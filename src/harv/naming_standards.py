import re
import uuid
import hashlib
from typing import Any, Literal


def format_column(*args: Any, mode: Literal["light", "full"] = "light") -> str:  # noqa: C901, ANN401
    r"""
    Converts given strings into a accepable format for Databricks.

    In light mode, it looks for special characters, namely [,;{}()\n\t= ]),
    which are not allowed in Databricks, and replaces them with underscores.
    It also replaces two or more consecutive underscores with a single underscore.

    In "full" mode, it performs additional operations:
    1. Handles PascalCase and CamelCase by inserting underscores before uppercase letters that are
    preceded by a lowercase letter or a digit, or any uppercase letter that is followed by a lowercase letter.
    2. Converts the string to lowercase.
    3. Replaces edge cases with corresponding alphanumeric characters.
    4. Replaces "\\n", "\\t", and "\\r" with underscores.
    5. Replaces all non-alphanumeric characters (except 'æ', 'ø', 'å') with underscores.
    6. Ensures the final string is no longer than 127 characters.

    Args:
        *args (Any): Variable length argument list of strings to be formatted. eg the colunm names.
        mode (Literal["light", "full"]): The formatting mode to apply. Defaults to "light".

    Returns:
        str: The formatted string.

    Raises:
        ValueError: If no arguments are provided or
                    if the input string is too long or
                    cannot be converted to snake_case.

    """
    if not args:
        msg = "At least one argument must be provided."
        raise ValueError(msg)

    names = []
    for arg in args:
        if arg:
            names.append(str(arg))
        else:
            unique_id = uuid.uuid4()
            short_id = hashlib.sha1(unique_id.bytes).hexdigest()[:10]
            names.append(short_id)
    name = "_".join(names)

    if mode == "full":
        name = re.sub(r"((?<=[a-z0-9æøå])[A-Z]|(?!^)[A-Z](?=[a-zæøå]))", r"_\1", name)
        name = name.lower()
        letters = dict(enumerate(name))

        for i, letter in letters.items():
            if letter in "áãâà":
                letters[i] = "a"
            elif letter in "éëêè":
                letters[i] = "e"
            elif letter in "íïîì":
                letters[i] = "i"
            elif letter in "óõôò":
                letters[i] = "o"
            elif letter in "úüûù":
                letters[i] = "u"
            elif letter in "ýÿ":
                letters[i] = "y"
            elif letter in "ñ":
                letters[i] = "n"
            elif letter in "ß":
                letters[i] = "ss"
            elif letter in "ç":
                letters[i] = "c"

        name = "".join(letters.values())
        name = name.replace("\\n", "_")
        name = name.replace("\\t", "_")
        name = name.replace("\\r", "_")
        name = re.sub(r"[^a-z0-9_æøå]+", "_", name)

    name = re.sub(r"[,;{}()\n\t= ]", "_", name)
    name = re.sub(r"_+", "_", name)
    name = name.strip("_")

    if mode == "full" and (len(name) > 127 or not re.match(r"^[a-z0-9_æøå]+(_[a-z0-9_æøå]+)*$", name)):
        msg = f"Cannot convert '{name}' to snake_case or it is too long(>127 chracters)."
        raise ValueError(msg)

    return name
