@echo off
perl trec_eval.pl qrels.adhoc.51-100.AP89.txt tfidf_results_index_3.txt > tfidf_result.txt
perl trec_eval.pl qrels.adhoc.51-100.AP89.txt bm25_results_index_3.txt > bm25_result.txt
perl trec_eval.pl qrels.adhoc.51-100.AP89.txt laplace_results_index_3.txt > laplace_result.txt