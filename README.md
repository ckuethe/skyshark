# airshark
Airshark is a python decoder for ACARS messages, roughly inspired by wireshark.
It parses JSON output from [acarsdec](https://github.com/TLeconte/acarsdec) and
attempts to guess the meaning of received messages. Where possible, it will
disambiguate messages types with multiple interpretations using other metadata,
such as aircraft registration or flight ID.
