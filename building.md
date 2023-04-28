# Building
In order to build this package you will need:

1. [`mdBook`](https://rust-lang.github.io/mdBook/) for building the help documentation
1. The `fpkgr`-utility from Safe Software for building the .fpkg.

On windows, something like following should work:

```
git clone https://github.com/eea/<TODO>
cd <TODO>
git checkout -b v_1.0.2
python update_version.py
git log -1 --pretty=format:%H > .commit_hash
mdbook build docs\help -d ..\..\help\pkg-eurostat
copy /y README.md formats\eurostat.md
fpkgr pack . 
```


