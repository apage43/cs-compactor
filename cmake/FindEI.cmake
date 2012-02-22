include(LibFindMacros)
execute_process(
    COMMAND erl -eval "io:put_chars(code:lib_dir(erl_interface)), erlang:halt()."
    OUTPUT_VARIABLE EIDIR
)
set(EI_INCLUDE_DIR "${EIDIR}/include")
set(EI_LIBRARY "${EIDIR}/lib")
set(EI_PROCESS_INCLUDES EI_INCLUDE_DIR)
set(EI_PROCESS_LIBS EI_LIBRARY)
libfind_process(EI)
