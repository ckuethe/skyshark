# systemd service files

These services assume you have created a `skyshark` user to own the files and
processes, and that its home directory is `/home/skyshark`. There are other
parameters in these service files that need to be customized per installation.

* `skyshark_acarsdec.service` - runs acarsdec and sends json logs to the loghost

* `skyshark_acars_loader.service` - listens for acarsdec json logs and stores them into mongodb

* `skyshark_adsb_loader.service` - polls a remote instance of dump1090 and stores SBS1 logs into mongodb
