import subprocess
import shlex
import datetime

next_date = (datetime.datetime.now() + datetime.timedelta(days=1)).date()
next_date = next_date.isoformat()
subprocess.call(shlex.split("timedatectl set-ntp false"))  # May be necessary
subprocess.call(shlex.split("sudo date -s '%s'" % next_date))
subprocess.call(shlex.split("sudo hwclock -w"))

# time_string = datetime()