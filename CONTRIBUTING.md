# Contributing

Want to contribute? Head over to our [Issue Tracker](https://github.com/thinknode/thinknode-provider-ipc-net/issues) to discuss your ideas. While we have no formal guidelines for coding standards, try to use the existing code as a guide. Then, when you are ready to submit a pull request, please make sure you squash your commits down to one commit with a short description of what the pull request accomplishes. For example, if you added something on the branch `really-cool-feature`, you might do the following.

```
git checkout master
git pull origin master
git checkout -b really-cool-feature-PR
git merge --squash really-cool-feature
git commit -m "Added a really cool feature"
git push really-cool-feature-PR
```