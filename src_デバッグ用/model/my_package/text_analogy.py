# -*- coding: utf-8 -*-
# Copyright 2020 FUJITSU SOFTWARE TECHNOLOGIES LIMITED.
# System: レセプト診断予測AI
# Date: 2020/6/30
def han2zen_KaAl(text, engine='cnvk'):
    """
    半角カタカナ、アルファベット、数字を全角に変換する。
    ライブラリが入っていない、mojimojiやzenhanなど別のライブラリや自作関数を使いたい等の場合は各々分岐を追加してください。
    作成時点ではcnvkを使っています。
    """
    # if engine == 'cnvk':
    """
    以下からダウンロードしたプログラムです。
    https://github.com/yuka2py/cnvk
    """
    from external_package.cnvk import cnvk
    return cnvk.convert(text, cnvk.Z_ALPHA, cnvk.Z_KATA)


def zen2han_Num(text, engine='cnvk'):
    """
    数字を半角に変換する。
    ライブラリが入っていない、mojimojiやzenhanなど別のライブラリや自作関数を使いたい等の場合は各々分岐を追加してください。
    作成時点ではcnvkを使っています。
    """
    # if engine == 'cnvk':
    """
    以下からダウンロードしたプログラムです。
    https://github.com/yuka2py/cnvk
    """
    from external_package.cnvk import cnvk
    return cnvk.convert(text, cnvk.H_NUM)


# %%
def large2small(text, engine='cnv'):
    """
    アルファベッドの大文字を小文字に変換する。
    """

    # new
    from external_package.cnvk import cnvk
    return cnvk.convert(text, cnvk.L_ALPHA)

    # old
    # return text.lower()
