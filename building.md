# Building
In order to build this package you will need:

1. [`mdBook`](https://rust-lang.github.io/mdBook/) for building the help documentation
1. The `fpkgr`-utility from Safe Software for building the .fpkg.

On windows, something like following should work:

```
git clone https://github.com/eea/<TODO>
cd <TODO>
for /f "usebackq delims==" %i IN (`python -c "from ruamel.yaml import YAML;print(YAML().load(open('package.yml')).get('version'))"`) DO SET MDBOOK_BOOK__TITLE=eea.eurostat [%i]
git log -1 --pretty=format:%H > .commit_hash
mdbook build doc\help -d ..\..\help\pkg-eurostat
fpkgr pack . 
```


