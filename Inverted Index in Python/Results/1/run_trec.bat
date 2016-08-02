@echo off
perl trec_eval.pl qrels.adhoc.51-100.AP89.txt tfidf_results_index_1.txt > tfidf_result.txt
perl trec_eval.pl qrels.adhoc.51-100.AP89.txt bm25_results_index_1.txt > bm25_result.txt
perl trec_eval.pl qrels.adhoc.51-100.AP89.txt laplace_results_index_1.txt > laplace_result.txt
