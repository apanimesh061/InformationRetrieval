/**

Jelenik-Mercer Scoring using Groovy Custom script

**/

def score = 0;

for(term in terms) {
  	term_freq = _index[field][term].tf();
  	tot_term_freq = _index[field][term].ttf();
  	document_len = doc[len_field].value;
  	exp_1 = lambda * (term_freq / document_len);
  	exp_2 = (1 - lambda) * (tot_term_freq / total_tokens);
  	score += log(exp_1 + exp_2);
};
score;