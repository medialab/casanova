import re
from textwrap import dedent

from casanova.__main__ import CASANOVA_COMMANDS, CASANOVA_PARSER

TEMPLATE_RE = re.compile(r"<%\s+([A-Za-z/\-]+)\s+%>")


def template_readme(tpl):
    def replacer(match):
        key = match.group(1)

        if key == "main":
            target = CASANOVA_PARSER
        else:
            target, _ = CASANOVA_COMMANDS[key]

        return (
            dedent(
                """
                    ```
                    %s
                    ```
                """
            )
            % target.format_help().strip()
        ).strip()

    return re.sub(TEMPLATE_RE, replacer, tpl)


if __name__ == "__main__":
    with open("./docs/cli.template.md") as f:
        template_string = f.read()

    print(template_readme(template_string))
