# Why is this folder copied from singer_sdk.samples?
Copied from singer_sdk.samples as `samples` isn't included as a part of the singer_sdk package. We want `samples` to do some testing with.

Some alternatives here would be potentially having `singer_sdk` include a `samples` package. Maybe `singer-sdk-samples` maybe via a `pyproject.toml` in the `samples/` directory?

Could also split samples out to a seperate repo, but that seems like a bit much!
