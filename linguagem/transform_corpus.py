import pandas as pd
import re
from tqdm import tqdm
import swifter
import spacy
from nltk.tokenize import wordpunct_tokenize

tqdm.pandas()
spacy_text_analyser = spacy.load('pt_core_news_md')


def term_frequency_from_corpus(corpus_list: list, term_frequency=None):
    term_frequency = dict() if term_frequency is None else term_frequency
    for corpus in corpus_list:
        for text in tqdm(corpus["corpus"]["text"]):
            for word in wordpunct_tokenize(text):
                if not word.isalpha():
                    continue
                word = word.lower()
                term_frequency[word] = term_frequency[word] + 1 if word in term_frequency.keys() else 1
    return sorted(term_frequency.items(), key=lambda x: x[1], reverse=True)


def aplicar_spacy(term_frequency):
    df_palavras = pd.DataFrame(term_frequency, columns=["palavra", "frequencia"])

    def apply_spacy(df_row):
        df_row["lemma"] = spacy_text_analyser(df_row["palavra"])[0].lemma_
        df_row["classe_gramatical"] = spacy_text_analyser(df_row["palavra"])[0].pos_
        return df_row

    # Aplicando Spacy para obter os lemmas (Similar ao radical, mas melhor que o do nltk snowball)
    # e a classe gramatical.
    df_palavras = (
        df_palavras
        .swifter
        .progress_bar(True, desc="spacy")
        .allow_dask_on_strings(enable=True)
        .force_parallel(enable=True)
        .apply(lambda x: apply_spacy(x), axis=1)
    )
    return df_palavras


def tratar_classes_gramaticais(df_palavras):
    # Substituindo alguns Lemmas que identifiquei com problema do spacy
    (
        df_palavras.loc[df_palavras["classe_gramatical"] == "AUX", "lemma"]
        .replace(
            {"havíamos": "haver",
             "tar": "estar",
             "seja": "ser",
             "estaríar": "estar",
             "esteja": "estar",
             "estiver": "estar"
             }, inplace=True
        )
    )
    df_palavras.loc[:, "classe_gramatical"] = df_palavras.loc[:, "classe_gramatical"].replace({"AUX": "VERB"})
    df_palavras = (
        df_palavras.loc[~df_palavras["classe_gramatical"].isin(["X", "SYM", "INTJ", "PROPN", "PUNCT", "PART", "NUM"])]
    )

    # Renomeando para melhor entendimento
    df_palavras.loc[:, "classe_gramatical"] = df_palavras.loc[:, "classe_gramatical"].replace(
        {
            "NOUN": "Substantivo",
            "VERB": "Verbo",
            "ADJ": "Adjetivo",
            "DET": "Artigo",
            "CCONJ": "Conjuncao",
            "SCONJ": "Conjuncao",
            "ADV": "Adverbio",
            "ADP": "Preposicao",
            "PRON": "Pronome"
        }
    )

    def apply_morph(df_row):
        df_row["Morph"] = spacy_text_analyser(df_row["palavra"])[0].morph
        return df_row

    df_palavras = (
        df_palavras
        .swifter
        .progress_bar(True, desc="morph")
        .allow_dask_on_strings(enable=True)
        .force_parallel(enable=True)
        .apply(apply_morph, axis=1)
    )
    morphs = set()
    for row in tqdm(df_palavras["Morph"].astype(str)):
        for found in re.findall(r"(?:^| |\|)(.*?)=", row):
            morphs.add(found)

    # Separando em dataframes por classe gramatical
    df_substantivos = df_palavras.loc[df_palavras["classe_gramatical"] == "Substantivo"]
    df_verbos = df_palavras.loc[df_palavras["classe_gramatical"] == "Verbo"]
    df_adjetivos = df_palavras.loc[df_palavras["classe_gramatical"] == "Adjetivo"]
    df_adverbios = df_palavras.loc[df_palavras["classe_gramatical"] == "Adverbio"]
    df_preposicoes = df_palavras.loc[df_palavras["classe_gramatical"] == "Preposicao"]
    df_pronomes = df_palavras.loc[df_palavras["classe_gramatical"] == "Pronome"]
    df_artigos = df_palavras.loc[df_palavras["classe_gramatical"] == "Artigo"]
    df_conjuncoes = df_palavras.loc[df_palavras["classe_gramatical"] == "Conjuncao"]

    def apply_morph_cols(df_row, cols):
        morph = spacy_text_analyser(df_row["palavra"])[0].morph
        for col in cols:
            df_row[col] = morph.get(col)
        return df_row

    # Salvando em colunas as características morfológicas das palavras em cada dataframe
    dfs = tuple()
    for df in (
            df_substantivos,
            df_verbos,
            df_adjetivos,
            df_adverbios,
            df_preposicoes,
            df_pronomes,
            df_artigos,
            df_conjuncoes,
    ):
        df = (
            df
            .swifter
            .progress_bar(True, desc="morphs")
            .allow_dask_on_strings(enable=True)
            .force_parallel(enable=True)
            .apply(lambda x: apply_morph_cols(x, morphs), axis=1)
        )

        df.drop(columns=["Morph"], inplace=True)
        for m in morphs:
            df[m] = df[m].astype(str).str.replace("[", "").str.replace("]", "").str.replace("'", "")
        dfs = dfs + (df,)

    return dfs
