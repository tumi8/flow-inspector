#!/usr/bin/env python

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'config'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lib'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'vendor'))

import mail

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print "Usage: %s <subject> <message>" % (sys.argv[0])
		sys.exit(-1)
	mail.send_mail(sys.argv[1], sys.argv[2])
