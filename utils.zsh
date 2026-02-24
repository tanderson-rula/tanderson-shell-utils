# General-purpose shell utilities

unalias output_dir_to_file 2>/dev/null
output_dir_to_file() {
  echo "type,path,name,extension" > dir_output.csv

  # Use a process substitution instead of a pipe (keeps same shell context)
  while IFS= read -r -d '' item; do
    if [[ -d "$item" ]]; then
      type="directory"
      path="${item%/*}"
      name="${item##*/}"
      ext=""
    elif [[ -f "$item" ]]; then
      type="file"
      path="${item%/*}"
      name="${item##*/}"
      ext="${name##*.}"
      [[ "$name" == "$ext" ]] && ext=""
    else
      continue
    fi

    # Escape commas for CSV safety
    safe_path="${path//,/\\,}"
    safe_name="${name//,/\\,}"
    safe_ext="${ext//,/\\,}"

    echo "$type,$safe_path,$safe_name,$safe_ext" >> dir_output.csv
  done < <(find . -print0)

  echo "Wrote directory structure to dir_output.csv"
}
