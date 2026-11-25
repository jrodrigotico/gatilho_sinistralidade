



class RegraParceiro:
    """
    Classe base para as regras específicas dos parceiros do Gatilho.

    As classes de cada parceiro devem herdar desta classe e implementar
    somente os comportamentos que forem específicos daquele parceiro.
    """

    def __init__(self, nome_parceiro):
        self.nome_parceiro = nome_parceiro