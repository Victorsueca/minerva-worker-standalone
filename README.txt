worker.py adapted from https://gist.github.com/rlaphoenix/257b7aa65adacc154d8b5fa0b035b1e8

Build:
    # Prepare dependencies
    `python -m venv .venv`
    `source .venv/bin/activate` # On Windows use `.venv/Scripts/activate` instead
    `pip install -r requirements.txt`
    `pip install -r build_requirements.txt`
    # Build portable version
    `python setup.py build`
    # Build MSI on Windows
    `python setup.py bdist_msi`

While the original worker.py is freely distributed, it didn't include a license,
consequently it is presumed to be unlicensed.

The original description follows:

Minerva DPN Worker — single-file volunteer download client.

Requirements:
    pip install httpx rich click pathvalidate pyjwt

Optional (faster downloads):
    Install aria2c: https://aria2.github.io/

Usage:
    python worker.py login                    # Authenticate with Discord
    python worker.py run                      # Start downloading
    python worker.py run -c 8 -b 24           # 8 concurrent, fetch 24 per batch
    python worker.py run --server http://...  # Custom server URL

Modifications to original:
- Version numbers have "a" appended to the end to differ from the original
  version numbers. Feature parity with original script's versions are matched.
- The auto-update system no longer automatically launches the updated version.
  This is a security risk. It now asks you to check the code and re-run.
- Files no longer save as just the filename, but instead save to a path that
  exactly mirrors the original URL path, including hostname. This is to avoid
  filename collisions.
- The default temp directory is actually inside C:/Temp so that it will be
  cleared on reboots instead of potentially persisting for a long time.
- Changed the starting few logs on run, "Minerva DPN Worker" now just says
  "Minerva Worker". Also lists the version and will show this on all of the
  commands, like status, login, run, etc.
- The Username of the logged in user is now printed on run.

More modifications by Puyodead1:
- Even if aria2c is installed, it will only be used for files larger than 5 MB.
  aria2c initialization overhead makes it slower for small files.
- aria2c connections can be configured with the new --aria2c-connections option.
- aria2c connect timeout is now 15s instead of 30s, to fail faster.
- HTTPX timeouts are now more fine tuned for each kind of timeout.
- General code surrounding TUI improved, separated from main/processing logic.
- Reworked the main loop to use a producer/consumer pattern with an asyncio.Queue,
  which is more robust and easier to manage than the previous ad-hoc approach.
- Will ask for more jobs while doing the current jobs, instead of waiting for all
  to finish first. This keeps the workers busier and improves throughput.

Bugs fixed in this version compared to original:
- Two URLs with the same filename would overwrite each other in the temp dir,
  causing upload failures or possibly uploading the wrong file, depends on how
  the server checks/verifies files, which is not known.
