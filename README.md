# split_client_side

A client side version of the [Split Python SDK](https://github.com/splitio/python-client),
suitable for distribution to untrusted environments.

## Installation

Clone this repository, `cd` into its directory, and `pip install -e .`

## Usage

```python
from split_client_side.client.factory import build_factory
from splitio.client.config import sanitize as build_default_config
from splitio.exceptions import TimeoutException

config = {'sql.url': 'sqlite:///split.sqlite'}  # persist Split definitions in a SQLite DB
config = build_default_config('CLIENT_SIDE_API_KEY', config)  # initialize with a client side API key
factory = build_factory('CLIENT_SIDE_API_KEY', config, 'TRAFFIC_KEY')  # factory and associated clients are bound to a single key

try:
    factory.block_until_ready(5) # wait up to 5 seconds
except TimeoutException:
    # Now the user can choose whether to abort the whole execution, or just keep going
    # without a ready client, which if configured properly, should become ready at some point.
    pass
split = factory.client()
treatment = split.get_treatment('SPLIT_NAME')
if treatment == "on": 
    # insert code here to show on treatment
elif treatment == "off":
    # insert code here to show off treatment
else:
    # insert your control treatment code here
```
