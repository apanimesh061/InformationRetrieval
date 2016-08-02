/**

Language Model Scoring using Groovy Custom script

**/

def score = 0;

for(term in terms) {
   term_freq = _index[field][term].tf();
   document_len = doc[len_field].value;
   p_laplace = (term_freq + 1)/(document_len + V);
   score += log(p_laplace);
};
score;