import os
import sys

"""

link this file to ~/.zipline/extension.py

"""

sys.path.append(os.path.join(__path__, '../'))

bundle_type = os.environ.get('bundle')

if bundle_type == 'crypto':
    from zipline.data.zipline_ingest.crypto.bitmex import *
    main()
elif bundle_type == 'future':
    from zipline.data.zipline_ingest.future.future_bundle_new import *
    main()
