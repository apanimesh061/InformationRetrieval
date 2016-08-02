/**

Okapi TF Scoring using Groovy Custom script

**/

def score = 0;

for(term in terms){ 
   	if (_index[field][term].tf() > 0){
   		def okapi_tf = 0; 
		term_freq = _index[field][term].tf(); 
		exp1 = term_freq + 0.5; 
		exp2 = _doc[len_field].value/_index[field][term].df(); 
		okapi_tf = term_freq/(exp1 + (1.5*exp2)); 
	    
	    score += okapi_tf
   	}
};
score;