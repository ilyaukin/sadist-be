import re

import pymongo
from pymongo.database import Database

from db import SadistDatabaseConnection


def upgrade(db: Database):
    template_list = list(db.wc_script_template.find())
    for template in template_list:
        data = template['data']
        # convert script/script template automatically whenever possible...
        # since function now is a function, change `getScript` method
        data = re.sub(r"new Function\('page', text\)",
                      "new Function('page', 'return ('.concat(text, ')(page);'))",
                      data)
        # replace `text` accordingly
        data = re.sub(r"\"text\":\"\s*return\s+\((.*)\)\(page\)[;]?\s*\",",
                      lambda match: f"\"text\":\"/**\\n * @param page {{Page}}\\n */\\n{match.group(1)}\",",
                      data)
        # replace all functions to async
        data = re.sub(r"\([^()]*\) =>",
                      lambda match: f"async {match.group(0)}",
                      data)
        # replace `querySelector` with `$`
        data = re.sub(r"(\w+)\.querySelector\(",
                      lambda match: f"await {match.group(1)}.$(",
                      data)
        # replace `querySelectorAll` with `$$`
        data = re.sub(r"(\w+)\.querySelectorAll\(([^()]*)\)",
                      lambda match: f"(await {match.group(1)}.$$({match.group(2)}))",
                      data)
        # replace `textContent` in "$useAsField" method...
        data = re.sub(r"\s*=\s*(await \w+\.\$\(\"[^\"]*\")\)\?\.textContent(\?\.trim\(\))?",
                      lambda match: f" = await page.evaluate((element) => element?.textContent{match.group(2) or ''}, {match.group(1)}))",
                      data)
        # replace `forEach` with
        # asynchronous call of each function
        data = re.sub(r"(\(await \w+\.\$\$\([^()]*\).*?\.)forEach\((.*?})\);",
                      lambda match: f"await Promise.all({match.group(1)}map({match.group(2)}));",
                      data)

        db.wc_script_template.update_one({'_id': template['_id']},
                                         {'$set': {'data': data}})


def downgrade(db: Database):
    pass


if __name__ == '__main__':
    upgrade(pymongo.MongoClient(SadistDatabaseConnection.DATABASE_URL).get_database())
