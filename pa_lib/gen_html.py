"""
Generate HTML fragments to use in Juypter notebooks

Examples:
    display(H1("Main Title"))
    display(HR(), H2("Subtitle"))
"""
from IPython.core.display import HTML


########################################################################################
def html(src: str) -> HTML:
    """Wraps any HTML code for display()"""
    return HTML(src)


########################################################################################
def HR() -> HTML:
    """Horizontal rule HTML fragment"""
    return HTML("<hr>")


########################################################################################
def H1(txt: str) -> HTML:
    """Level 1 title HTML fragment"""
    return HTML(f"<h1>{txt}</h1>")


def H2(txt: str) -> HTML:
    """Level 2 title HTML fragment"""
    return HTML(f"<h2>{txt}</h2>")


def H3(txt: str) -> HTML:
    """Level 3 title HTML fragment"""
    return HTML(f"<h3>{txt}</h3>")
