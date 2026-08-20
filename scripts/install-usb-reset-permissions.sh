#!/bin/sh
set -eu

rule_source=$(CDPATH= cd -- "$(dirname -- "$0")/../udev" && pwd)/99-sik-link-ft230x.rules
sudo install -o root -g root -m 0644 "$rule_source" /etc/udev/rules.d/99-sik-link-ft230x.rules
sudo udevadm control --reload-rules
sudo udevadm trigger --subsystem-match=usb --attr-match=idVendor=0403 --attr-match=idProduct=6015
echo "Installed SiK FT230X USB-reset permissions for group dialout."
