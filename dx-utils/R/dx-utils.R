dx_system = function(args, verbose=F) {
  if(verbose) {
    message(glue::glue("Command: {paste0(c('dx', args), collapse=' ')}"))
  }
  out = invisible(system2("dx", args, stdout=T))
  return(out)
}

dx_ensure = function() {
  if (nzchar(Sys.which("dx")) == FALSE) {
    stop("The 'dx' command-line tool was not found on PATH. Please install/configure DNAnexus CLI.")
  }
}

dx_ok = function(dx_out, verbose=F) {
  status = attr(dx_out, "status")
  if(verbose) {
    message(glue::glue("Return: {status}"))
  }
  invisible(is.null(status) || identical(status, 0L))
}

dx_mkdir = function(path, verbose=F) {
  if(path %in% c(".", "")) return(NULL)
  mkdir_out = dx_system(c("mkdir", "-p", path), verbose=verbose)
  if(!dx_ok(mkdir_out, verbose=verbose)) {
    stop(glue::glue("dx mkdir failed"))
  }
}

dx_rm_ids = function(ids, verbose=F) {
  rm_out = dx_system(c("rm", ids), verbose=verbose)
  if(!dx_ok(rm_out, verbose=verbose)) {
    stop(glue::glue("dx rm failed"))
  }
}

dx_ls = function(remote_path, files=T, folders=T, verbose=F) {
  dx_ensure()
  if (missing(remote_path)) {
    stop("Usage: dx_ls(remote_folder, files=TRUE, folders=TRUE)")
  }

  out = dx_system(c("ls", remote_path), verbose=verbose)
  if (!dx_ok(out, verbose = verbose)) {
    stop(glue::glue("Failed to list contents of {remote_path}"))
  }

  lines = trimws(out)
  lines = lines[nzchar(lines)]

  is_folder = grepl("/$", lines)
  keep = (folders & is_folder) | (files & !is_folder)
  lines[keep]
}

dx_exist = function(remote_path, is_folder=T, is_file=T, verbose=F) {
  dx_ensure()
  if (missing(remote_path)) {
    stop("Usage: dx_exist(remote_folder, is_folder=TRUE, is_file=TRUE, verbose=FALSE)")
  }
  ls_contents = dx_ls(dirname(remote_path), verbose=verbose, folders=is_folder, files=is_file)
  basename(remote_path) %in% basename(ls_contents)
}

dx_file_ids = function(remote_file_path, verbose=F) {
  dx_ensure()

  if (missing(remote_file_path)) stop("Usage: dx_file_id(remote_file_path)")
  folder = dirname(remote_file_path);

  if (folder %in% c(".", "/")) folder = "/"
  name  = basename(remote_file_path)

  ids_out = invisible(dx_system(c("find", "data", "--path", folder, "--name", name, "--brief"), verbose=verbose))
  if(!dx_ok(ids_out, verbose=verbose)) {
    return(invisible(c("")[0]))
  }

  ids = trimws(ids_out)
  ids = ids[nzchar(ids)]

  invisible(ids)
}

dx_upload = function(local_path, remote_path, verbose=F) {
  dx_ensure()

  if (missing(local_path) || missing(remote_path)) {
    stop("Usage: dx_upload(local_path, remote_path)")
  }

  if (!file.exists(local_path)) {
    stop(glue::glue("Local path does not exist: {local_path}"))
  }


  local_abs = normalizePath(local_path, mustWork=T)
  l_is_dir = dir.exists(local_abs)
  r_is_dir = grepl("/$", remote_path) || l_is_dir

  if (l_is_dir) {
    local_files = list.files(local_abs, recursive=T, include.dirs=F, all.files=T, no..=T, full.names=T)
  } else {
    local_files = local_abs
  }

  # Process each file: delete all existing matches, then upload fresh
  for (lf in local_files) {
    lname = basename(lf)
    ldir = dirname(lf)
    lpath = glue::glue("{ldir}/{lname}")

    rname = ifelse(l_is_dir || r_is_dir, lname, basename(remote_path))
    rdir = ifelse(r_is_dir, dirname(glue::glue("{remote_path}/{fs::path_rel(lf, start=local_path)}")), dirname(remote_path))
    rpath = glue::glue("{rdir}/{rname}")

    # We create a temporary directory, copy file there and only then upload.
    # This is done because when uploading files to DNANexus we can't change their name
    # Therefore we rename first and then upload
    tmp_dir = tempfile(pattern = "mytemp_")
    dir.create(tmp_dir)
    if(rname != lname) {
      lpath = file.path(tmp_dir, rname)
      file.copy(lf, lpath)
    }

    # Ensure parent folder exists remotely
    dx_mkdir(rdir, verbose=verbose)
    ids = dx_file_ids(rpath, verbose=verbose)
    if (length(ids)) {
      rm_out = dx_system(c("rm", ids), verbose=verbose)
      dx_ok(rm_out, verbose=verbose)
    }

    upload_args = c("upload", lpath)
    if(rdir != "") upload_args = c(upload_args, "--destination", glue::glue("{rdir}/"))
    upload_out = invisible(dx_system(upload_args, verbose=verbose))
    if(!dx_ok(upload_out, verbose=verbose)) {
      warning(glue::glue("Failed to upload {lf}"))
    }

    unlink(tmp_dir, recursive=T)
  }

  invisible(TRUE)
}

dx_download = function(remote_path, local_path, verbose=F) {
  dx_ensure()
  if (missing(remote_path) || missing(local_path)) {
    stop("Usage: dx_download(remote_path, local_path)")
  }

  r_is_dir = grepl("/+$", remote_path)
  l_is_dir = grepl("/+$", local_path)
  rname = gsub('[\\\\\\/$]*$', '', remote_path)
  lname = gsub('[\\\\\\/$]*$', '', local_path)

  ldir_tmp = ""
  ldir = ifelse(l_is_dir, gsub("[\\\\\\/]*$", "", local_path), dirname(local_path))
  tmp_dir = tempfile(pattern = "mytemp_")
  dir.create(tmp_dir, recursive=T, showWarnings=F)
  if(l_is_dir) {
    dir.create(ldir, recursive=T, showWarnings=F)
  } else {
    if(basename(remote_path) != basename(local_path)) {
      ldir_tmp = ldir
      ldir = tmp_dir
    }
  }

  args = c("download", "--recursive", "--overwrite", "--output", ldir, remote_path)
  dl_out  = invisible(dx_system(args, verbose=verbose))
  if (!dx_ok(dl_out, verbose = verbose)) {
    stop(glue::glue("Failed to download {remote_path}"))
  }

  if(basename(remote_path) != basename(local_path)) {
    if(l_is_dir) {
      dir.create(local_path, recursive=T, showWarnings=F)
      file.rename(glue::glue("{ldir}/{rname}"), local_path)
      file.copy(glue::glue("{ldir}/{lname}"), glue::glue("{ldir_tmp}/{lname}"))
    } else {
      dir.create(dirname(local_path), recursive=T, showWarnings=F)
      file.rename(glue::glue("{ldir}/{basename(rname)}"), glue::glue("{ldir}/{basename(lname)}"))
      file.copy(glue::glue("{ldir}/{basename(lname)}"), ldir_tmp, recursive=T, overwrite=T)
    }
    unlink(ldir)
  }

  invisible(TRUE)
}

dx_download_libraries = function(R_env, overwrite=TRUE, quiet=FALSE) {
  dx_ensure()
  stopifnot(is.character(R_env), length(R_env) == 1L, nzchar(R_env))

  # Resolve the local path: ~/R_libs/<R_env>
  local_dir = path.expand(file.path("~", "R_libs", R_env))
  dir.create(local_dir, recursive=T, showWarnings=F)

  remote_dir = glue::glue("R_libs/{R_env}")

  # Run dx download and check for errors
  remote_exist = dx_exist(remote_dir)
  if(remote_exist) {
    dx_args = c("download", "-r", remote_dir, "-o", dirname(local_dir))
    if (overwrite) dx_args = c(dx_args, "--overwrite")
    if (quiet)     dx_args = c(dx_args, "--no-progress")
    dx_out = tryCatch(system2("dx", dx_args, stdout=T, stderr=T), error=function(e) structure(character(), status=1L))

    if(!dx_ok(dx_out, verbose=verbose)) {
      stop(glue::glue("dx download failed"))
    }
  } else {
    dir.create(local_dir, showWarnings=F, recursive=T)
  }

  .libPaths(c(local_dir, .libPaths()))

  required_libraries = c("stringr", "remotes", "readr", "fs")
  for(lib in required_libraries) {
    if(lib %in% rownames(installed.packages()) == FALSE) {
      install.packages(lib, lib=local_dir, quiet=T, repos="http://cran.us.r-project.org")
    }
  }
}

dx_upload_libraries = function(R_env, quiet=FALSE) {
  stopifnot(is.character(R_env), length(R_env) == 1L, nzchar(R_env))
  dx_ensure()

  local_dir  = path.expand(file.path("~", "R_libs", R_env))
  remote_dir = glue::glue("R_libs/{R_env}")

  # Upload local libraries folder to project
  if (dir.exists(local_dir)) {
    up_args = c("upload", "-r", local_dir, "--destination", remote_dir, "-p")
    if (quiet) up_args = c(up_args, "--no-progress")
    up_out = tryCatch(system2("dx", up_args, stdout=T, stderr=T), error = function(e) structure(character(), status=1L))
    if(!dx_ok(up_out, verbose=verbose)) {
      stop(glue::glue("dx upload failed"))
    }
  } else {
    message(glue::glue("Local directory '{local_dir}' does not exist; created remote folder '{dest_path(remote_dir)}' only."))
  }
}