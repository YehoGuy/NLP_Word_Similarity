# NLP_Word_Similarity - By Guy Yehoshua

## System Design 
The system is implemented as three Map‑Reduce stages:

**Association‑count stage** – For every lexeme–feature pair we tally all frequency statistics required for association measures: count(L), count(F), count(l,f), count(L = l), count(F = f).
Input: lines describing dependency parse trees.
Output: records <lexeme, feature‑label, count>.
The mapper also keeps Count(L) and Count(L = l) in memory, and—because of memory limits—filters so that only words appearing in the gold‑standard set are processed.

**Association‑score stage** – For each lexeme we compute the chosen association measures (raw frequency, relative frequency, PMI, t‑test) for all of its features and aggregate the results.
Output: records HashMap<feature, [count, freq, PMI, T‑Test]> per lexeme.

**Similarity‑vector stage** – A three‑way self‑join merges these records with the (filtered) word‑pair file, and for every relevant pair it produces a 24‑dimensional vector containing all combinations of the four association measures × six vector‑similarity measures.
Example output:


### Understanding the Input Dataset's Structure - Google's English All Biarcs dataset.

Items in the “biarcs” dataset reflect higher-order dependency relations that involve two arcs -- three connected content words.  
The biarcs datasets allows modeling information involving interactions between two modifiers of the same head, e.g. subject and object of the same verb (e.g (boy, ate, cookies)), as well as complex arguments of a head, e.g. adjectivial modifier of a verb’s argument (e.g ((small, boy), ate)).
By abstracting over the middle element, we could also get second-order information, e.g. (boy, *, cookies) and ((small, *), ate).

By further abstracting over the words, one may uncover second-order syntactic phenomena: maybe some verbs are more likely to have adjectives to their subjects than others?

**Example**:
experience      that/IN/compl/3 patients/NNS/nsubj/3 experience/VB/ccomp/0      3092

We can break this down (schematically) into:

- **“experience”** (the main word or one of the dependents)
- **“that/IN/compl/3”** (word + POS + dependency relation + index in the sentence)
- **“patients/NNS/nsubj/3”** (word + POS + dependency relation + index)
- **“experience/VB/ccomp/0”** (the main word/central verb + POS + relation + index)
- **3092** (a count—how many times this structure appears)

### Understanding The Article "Comparing Measures of Semantic Similarity"
1.2 -- 
Short Explanation of F and L in Section 1.2

In that section of the article, F and L are simply two different random variables:

L = the “lexeme” or “target word”
F = the “feature” (context word)

Whenever you see something like P(F = f | L = l), it means “the probability of feature f given that our target lexeme is l.”

### Statistics – 10 % Corpus Run 
``` :contentReference[oaicite:2]{index=2}:contentReference[oaicite:3]{index=3}  

Stage	Mapper in	Mapper out	Combiner out	Reducer out
1	159,033,091 records (4.14 GB)	178,902,389 records (63 MB)	7,065,206	3,811,348
2	3,811,348	3,261,318 records (173 MB)	0	414
3	414	2,349,864 records (11.15 TB virtual)	0	48

Classification Results (WEKA SGDText, 10‑fold CV) 

Overall accuracy: 91.67 % (44 / 48 instances)

Kappa: 0.75 MAE: 0.0833 RMSE: 0.2887

Class	Recall	Precision	F‑Measure	ROC AUC
similar	66.7 %	100 %	0.80	0.833
not_similar	100 %	90 %	0.947	0.833
Weighted avg.	91.7 %	92.5 %	0.911	0.862

Confusion matrix

a  b  <-- classified as
8  4 | a = similar
0 36 | b = not_similar

Summary: 44 of 48 instances were correctly classified, yielding 91.67 % accuracy. The model retrieves all not‑similar pairs (100 % recall) at 90 % precision, while for similar pairs it reaches 100 % precision with 66.7 % recall (F1 = 0.8
```

### Guy Yehoshua, Software Engineering B.Sc.
