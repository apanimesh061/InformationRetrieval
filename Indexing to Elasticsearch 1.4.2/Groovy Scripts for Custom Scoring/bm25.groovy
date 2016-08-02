/**

BM-25 Scoring using Groovy Custom script

**/


def score = 0;
for (term in terms) {
	q_term_freq = terms.countBy{ it }[term];
	if (_index[field][term].tf() > 0) {
		term_freq = _index[field][term].tf();
		doc_freq = _index[field][term].df();
		exp_1 = log((no_of_docs + 0.5) / (doc_freq + 0.5));
   		exp_2_n = term_freq * (1 + k1);
   		exp_2_d = term_freq + (k1 * ((1 - b) + (b * (_doc[len_field].value / avg_doc_len))));
   		exp_3_n = q_term_freq * (1 + k2);
   		exp_3_d = q_term_freq + k2;

   		score += exp_1 * (exp_2_n * exp_3_n) / (exp_2_d * exp_3_d);
	}
};
score;