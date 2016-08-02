# -------------------------------------------------------------------------------
# Name:        get_term_vectors
# Purpose:     getting feature values from emails
#
# Author:      Animesh Pandey
#
# Created:     29/04/2015
# Copyright:   (c) Animesh Pandey 2015
# -------------------------------------------------------------------------------

import constants
import es_utility
import csv, cPickle

if __name__ == '__main__':
    try:

        FEATURE_FILE = open('new_spam_features.csv', 'wb')
        EMAIL_TYPE_MAP = cPickle.load(open('email_type_map.pkl', 'rb'))
        spam_phrases = []
        with open('spam_terms.txt', 'rb') as sf:
            for phrase in sf:
                spam_phrases.append(es_utility.getRootQuery(phrase.strip()))
        SPAM_TERMS = set(sum(spam_phrases, []))
        csv.writer(FEATURE_FILE, delimiter=',').writerow(list(SPAM_TERMS))

        for count, filename in enumerate(constants.EMAIL_DOC_LIST):
            email = filename.split("\\")[1]
            try:
                email_data = es_utility.getTermVector(email, ['text', 'subject'])
            ##                print email_data['found'], email
            except Exception as e:
                print e
                continue
            if email_data['found']:
                # flags to check whther the email has text and a subject
                text_found = False
                subject_found = False

                try:
                    text_terms = email_data['term_vectors']['text']['terms'].keys()
                    text_found = True
                except KeyError as e:
                    # text wasn't found
                    pass
                try:
                    subject_terms = email_data['term_vectors']['subject']['terms'].keys()
                    subject_found = True
                except KeyError as e:
                    # subject wasn't found
                    pass
                if not text_found and not subject_found:
                    # there is not data to test on, so
                    # skip this document
                    continue
                all_terms = text_terms + subject_terms

                # create a the features
                feature = [email]
                for term in SPAM_TERMS:
                    feature.append(int(term in all_terms))

                try:
                    label = EMAIL_TYPE_MAP[email]
                except KeyError:
                    continue

                feature.append(label)

                # write the FEATURE values to a file
                csv.writer(FEATURE_FILE, delimiter=',').writerow(feature)
            else:
                continue

            curr_file = count + 1
            if curr_file % 1000 == 0:
                print "Covered {0} documents.".format(curr_file)
    except KeyboardInterrupt:
        print "There was a keyboard interrupt"
    finally:
        FEATURE_FILE.close()
