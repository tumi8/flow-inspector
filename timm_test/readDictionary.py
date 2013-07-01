#!/usr/bin/env python
# -*- coding: utf-8 -*-

# read csv file and parse it to dictionary
def readDictionary(file):

	# create dictionary for output
	dict = {}

	# open file and iterate over all lines expects empty ones
	f = open(file, 'r')
	for line in f:
		if line != "\n":

			# split line into segments
			items = line.strip().split(";")

			# parse last item 
			tmp_dict = __parseToken(items[-1])

			# create hierachy
			for item in items[:-1:][::-1]:
				tmp_dict = {item: tmp_dict}
			
			# merge temporaly built dictionary into output dictionary
			dict.update(tmp_dict)
	return dict

# parse last token a line
def __parseToken(token):

	return {
		's': lambda x: x,
		'f': lambda x: globals()[x],
		't': lambda x: tuple(map(__parseToken, x.split(","))),
		'n': lambda x: None 
	}[token[0]](token[1:])


# debugging stuff
def func1():
	 print "Func1"

def func2():
	print "Func2"

print readDictionary("test.csv")

readDictionary("test.csv")["timm"]()
