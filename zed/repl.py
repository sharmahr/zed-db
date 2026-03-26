"""
Zed Database Engine - REPL

Interactive SQL REPL using prompt_toolkit.
"""

from prompt_toolkit import prompt
from prompt_toolkit.history import InMemoryHistory
from prompt_toolkit.formatted_text import HTML
from prompt_toolkit import print_formatted_text

from zed.sql import Parser, ParseError
from zed.engine import Engine


def print_banner():
    """Print the welcome banner."""
    banner = HTML("""
<ansicyan>╔══════════════════════════════════════════════════════════════╗</ansicyan>
<ansicyan>║</ansicyan>          <ansibrightgreen>Zed Database Engine v0.1.0</ansibrightgreen>                <ansicyan>║</ansicyan>
<ansicyan>║</ansicyan>          <ansiyellow>A Python Relational Database</ansiyellow>                       <ansicyan>║</ansicyan>
<ansicyan>╚══════════════════════════════════════════════════════════════╝</ansicyan>

Type SQL commands to interact with the database.
Type <ansibrightred>quit</ansibrightred> or <ansibrightred>exit</ansibrightred> to leave.
""")
    print_formatted_text(banner)


def print_message(text: str):
    """Print a formatted message."""
    print_formatted_text(HTML(text))


def format_result(result: dict) -> str:
    """Format query result for display."""
    if result.get("status") == "error":
        return f"<ansired>✗ {result.get('message', 'Error')}</ansired>"
    
    if "rows" in result:
        # SELECT result
        columns = result.get("columns", [])
        rows = result.get("rows", [])
        count = result.get("count", 0)
        
        if not columns:
            return "<ansigreen>✓ Empty result set</ansigreen>"
        
        # Build table
        lines = []
        header = " | ".join(columns)
        lines.append(f"<ansicyan>{header}</ansicyan>")
        lines.append("-" * len(header))
        
        for row in rows:
            row_str = " | ".join(str(row.get(c, "")) for c in columns)
            lines.append(row_str)
        
        lines.append(f"<ansigreen>{count} row(s)</ansigreen>")
        return "\n".join(lines)
    
    else:
        # CREATE/INSERT result
        return f"<ansigreen>✓ {result.get('message', 'OK')}</ansigreen>"


def run_repl():
    """
    Run the interactive REPL.
    
    Parses SQL input, executes it against the engine, and displays results.
    """
    print_banner()
    
    engine = Engine()
    history = InMemoryHistory()
    
    while True:
        try:
            # Get user input
            user_input = prompt(
                HTML("<ansibrightblue>zed> </ansibrightblue>"),
                history=history,
            )
            
            text = user_input.strip()
            
            # Exit commands
            if text.lower() in ("quit", "exit"):
                print_message("<ansiyellow>Goodbye!</ansiyellow>")
                break
            
            if not text:
                continue
            
            # Parse SQL
            try:
                parser = Parser(text)
                statements = parser.parse()
            except ParseError as e:
                print_message(f"<ansired>Parse error: {e}</ansired>")
                continue
            
            if not statements:
                print_message("<ansired>No statement parsed</ansired>")
                continue
            
            # Execute each statement
            for stmt in statements:
                try:
                    result = engine.execute(stmt)
                    print_message(format_result(result))
                except Exception as e:
                    print_message(f"<ansired>Execution error: {e}</ansired>")
            
        except KeyboardInterrupt:
            print_message("\n<ansiyellow>Use 'quit' or 'exit' to leave.</ansiyellow>")
            continue
        except EOFError:
            print_message("\n<ansiyellow>Goodbye!</ansiyellow>")
            break


if __name__ == "__main__":
    run_repl()
