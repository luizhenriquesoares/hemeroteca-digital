"""Leitura visão-primeiro de páginas de jornal via Claude e benchmark associado.

O pipeline clássico (Tesseract -> correção LLM -> regex) é substituído por uma
única leitura da imagem que devolve transcrição por segmento editorial e a
estrutura (pessoas, cargos, lugares, embarcações, relações) com proveniência.
"""
