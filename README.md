# NLP_Word_Similarity - By Guy Yehoshua

### Input Dataset Structure - Google's English All Biarcs dataset.

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
- **3092** (a count—how many times this pattern appears)


### Guy Yehoshua, Software Engineering B.Sc.