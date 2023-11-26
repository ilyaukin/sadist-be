from pymongo.database import Database


def upgrade(db: Database):
    db.wc_proxy.insert_many([
        {'host': 'proxy1', 'port': 9222}
    ])
    db.wc_script_template.insert_many([
        {
            '_id': 'simple single-page scrapper',
            'readonly': True,
            'data': """{"name":"simple single-page scrapper","text":"return (function scratch(page) {\\n  return page.goto(\u003C%url%\u003E)\\n      .then(() =\u003E {\\n        const r = [[\u003C%title%\u003E]];\\n        page.querySelectorAll(\u003C%row%\u003E)\\n            .forEach((row, i) =\u003E {\u003C%fields%\u003E\\n              r.push([\u003C%values%\u003E]);\\n            });\\n        return r;\\n      });\\n})(page)","$Use as row":function $UseAsRow(s) {
        this.__replace('row', JSON.stringify(s));
      },"$Use as field":function $UseAsField(s, name) {
        var varName = name.replace(/[^a-zA-Z0-9$]+/g, '_');
        this.__add('fields', "\\n              const ".concat(varName, " = row.querySelector(").concat(JSON.stringify(s), ")?.textContent?.trim();"));
        this.__add('title', " ".concat(JSON.stringify(name), ", "), true);
        this.__add('values', " ".concat(varName, ", "), true);
      },"getUrl":function getUrl() {
        return this.__url;
      },"setUrl":function setUrl(url) {
        this.__url = url;
        this.__replace('url', JSON.stringify(url));
      },"getScriptText":function getScriptText() {
        return this.__removeplaceholders();
      },"getScript":function getScript() {
        var text = this.getScriptText();
        var fn = new Function('page', text);
        return {
          execute: function execute(page) {
            return fn(page);
          }
        };
      },"__replace":function __replace(placeholder, value) {
        var lborder = "<%".concat(placeholder);
        var rborder = "%>";
        var start = this.text.indexOf(lborder);
        var stop = this.text.indexOf(rborder, start);
        if (start >= 0 && stop >= 0) {
          // we keep placeholders to make this script template editable
          // with parameters; but it means that values cannot contain
          // placeholder-like sequences
          this.text = this.text.substring(0, start) + lborder + value + this.text.substring(stop);
        }
      },"__add":function __add(placeholder, value) {
        var isTrimEnd = arguments.length > 2 && arguments[2] !== undefined ? arguments[2] : false;
        var lborder = "<%".concat(placeholder);
        var rborder = "%>";
        var start = this.text.indexOf(lborder);
        var stop = this.text.indexOf(rborder, start);
        if (start >= 0 && stop >= 0) {
          var prevText = this.text.substring(0, stop);
          if (isTrimEnd) {
            prevText = prevText.trimEnd();
          }
          this.text = prevText + value + this.text.substring(stop);
        }
      },"__removeplaceholders":function __removeplaceholders() {
        return this.text.replace(/<%\w+/g, '').replace(/%>/g, '');
      }}"""
        }
    ])


def downgrade(db: Database):
    db.wc_proxy.delete_many({})
    db.wc_script_template.delete_many({})
