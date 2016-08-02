#-------------------------------------------------------------------------------
# Name:        module1
# Purpose:
#
# Author:      Animesh
#
# Created:     01/05/2015
# Copyright:   (c) Animesh 2015
# Licence:     <your licence>
#-------------------------------------------------------------------------------

import cPickle
import csv

##type_map = dict()
##with open('./full/index', 'rb') as ff:
##    for line in ff:
##        email_type, email = line.split()
##        type_map.update({email.strip().split('/')[-1] : 0 if email_type == 'spam' else 1})
##
##cPickle.dump(type_map, open('email_type_map.pkl', 'wb'))

spam = open('spam_features.csv', 'rb')
new_spam = open('new_spam_features.csv', 'wb')

