import itertools
import random
from tqdm import tqdm

from linguagem.fonemas import fonemas

random.seed(1)


class Silaba:
    def __init__(self, chars, tipo):
        self.chars = chars
        self.tipo = tipo

    def __str__(self):
        return "".join([fonemas[char] for char in self.chars])

    def __eq__(self, other):
        return self.chars == other.chars


class Palavra:
    def __init__(self, silabas, usada=False):
        self.silabas = silabas
        self.tamanho = len(silabas)
        self.usada = usada
        self.tipos_silabas = [silaba.tipo for silaba in silabas]

    def __str__(self):
        return "-".join([silaba.__str__() for silaba in self.silabas])

    def __eq__(self, other):
        return all([silaba_1 == silaba_2 for silaba_1, silaba_2 in zip(self.silabas, other.silabas)])


class Dicionario:
    def __init__(self):
        self.dict_1_2 = dict()
        self.dict_2_1 = dict()


class Linguagem:
    def __init__(self, categorias_caracteres, regras_silabais, precedencias_silabas):
        self.categorias_caracteres = categorias_caracteres
        self.regras_silabais = regras_silabais
        self.precedencias_silabas = precedencias_silabas

        self.silabas_dict, self.silabas_list = self.gerar_silabas()
        self.dicionario = Dicionario()

    def gerar_silabas(self):
        silabas_dict = dict()
        silabas_list = list()
        for n, regra in self.regras_silabais.items():
            regra_caracteres = list()
            for tipo_char in regra:
                regra_caracteres.append(self.categorias_caracteres[tipo_char])
            silabas = [Silaba(chars, n) for chars in list(itertools.product(*regra_caracteres))]
            silabas_dict[n] = silabas
            silabas_list += silabas
        return silabas_dict, silabas_list

    def gerar_palavra_aleatoria(self, n_silabas):
        silabas = list()
        tipo_silaba = 0
        for idx in range(n_silabas):
            tipo_silaba = random.choice(self.precedencias_silabas[tipo_silaba])
            silabas.append(random.choice(self.silabas_dict[tipo_silaba]))
        return Palavra(silabas)

    def gerar_todas_palavras_n(self, n_silabas):
        # Funciona, mas é muito lento para palavras de 3 sílabas ou mais.
        # Não vale a pena, vai ter q ser a outra abordagem.
        palavras = list()
        palavras_atual = list()
        palavras_antes = (
            [Palavra([silaba])
                for silabas in [
                self.silabas_dict[tipo_silaba] for tipo_silaba in self.precedencias_silabas[0]
            ]
                for silaba in silabas
            ]
        )
        if n_silabas == 1:
            return palavras_antes
        for i in range(1, n_silabas):
            for palavra_antes in tqdm(palavras_antes):
                tmp = (
                    [Palavra(palavra_antes.silabas + [silaba])
                     for silabas in [
                         self.silabas_dict[tipo_silaba]
                         for tipo_silaba in self.precedencias_silabas[palavra_antes.silabas[-1].tipo]
                     ]
                     for silaba in silabas]
                )
                palavras_atual = palavras_atual + tmp if palavras_atual else tmp
            palavras = palavras_atual
            palavras_atual = list()
            palavras_antes = palavras
        return palavras

    def adicionar_palavra(self, df_dicionario):
        pass
