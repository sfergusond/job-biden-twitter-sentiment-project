import sys, pandas as pd

sent = sys.argv[1]

df = pd.read_csv('sentiment.csv')
sent_df = df[df['sentiment'] == sent]
sent_df = sent_df[sent_df['language'] == 'en']

txt = '\n'.join(list(sent_df['clean_text'].astype('str')))
desc = '\n'.join(list(sent_df['clean_description'].astype('str')))

txt_file = open(f'{sent}_txt.txt', 'w')
desc_file = open(f'{sent}_desc.txt', 'w')

txt_file.write(txt)
desc_file.write(desc)