# airshark
Airshark is a python decoder for ACARS messages, roughly inspired by
wireshark. It parses JSON output from
[acarsdec](https://github.com/TLeconte/acarsdec) and attempts to guess
the meaning of received messages. Where possible, it will disambiguate
messages types with multiple interpretations using other metadata, such
as aircraft registration or flight ID.

Eventually I'll post some other tooling that uses
[MongoDB](https://github.com/mongodb/mongo) as its backing store for
aircraft registration (not every country is as nice as the US about
publishing downloadable databases), as well as ADSB observations from
[dump1090](https://github.com/mutability/dump1090)
