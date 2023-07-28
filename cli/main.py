import click
#  from prompt_toolkit import prompt
#  from prompt_toolkit import HTML, PromptSession, print_formatted_text
#  from prompt_toolkit.application import get_app
#  from prompt_toolkit.document import Document
#  from prompt_toolkit.shortcuts import CompleteStyle, print_container
#  from prompt_toolkit.validation import Validator
#  import driver.aws.providers
#  from cli.data_product import get_io_type

from cli.data_product import create_data_product

@click.group()
def cli():
    pass

# @click.group()
#  def data_lake():
#      pass


#  @click.command()
#  @click.option('-n', '--name', required=True, type=str)
#  @click.option('-l', '--location', required=True, type=str)
#  @click.option('-d', '--description', required=False, type=str)
#  @click.option('-t', '--tag', required=False, type=(str, str), multiple=True)
#  def create(name: str, location: str, description: str = None, tag: tuple = None):
#      click.echo(f'Creating a data lake: {name} at {location}')

# todo: remove spaces and other stuff upon finishing the line enry: https://github.com/prompt-toolkit/python-prompt-toolkit/blob/master/examples/prompts/autocorrection.py

#  filter = has_completions & ~completion_is_selected

#  @bindings.add(".")
#  def _(event: KeyPressEvent):
#      #  print(f'o---> {event}')
#      b = event.app.current_buffer
#      #  b.complete_previous()
#      b.insert_text('.')
#      #  if b.complete_state:
#      #  print(f'o> {b.complete_state}')
#      #  b.complete_next()
#      #  b.go_to_completion(0)
#      #  b.validate_and_handle()
#      if b.complete_state:
#         b.complete_next()
#      b.start_completion(select_first=True)
#      #  b.complete_next() if b.complete_state else b.start_completion(select_first=False)

#      #  w = b.document.get_word_before_cursor()

#      #  if w is not None:
#      #      if w in corrections:
#      #          b.delete_before_cursor(count=len(w))
#      #          b.insert_text(corrections[w])

@click.group(name="dp")
def data_product():
    pass
def main():
    #  cli.add_command(data_lake)
    #  data_lake.add_command(create)
    cli.add_command(data_product)
    data_product.add_command(create_data_product)
    cli()

if __name__ == "__main__":
    main()
