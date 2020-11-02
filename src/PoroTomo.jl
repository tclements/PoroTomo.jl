module PoroTomo

using AWS, Dates, SeisIO, SeisIO.Nodal
using AWS: @service
@service S3
export get_porotomo_files, porotomo_stream, porotomo_download
aws_config = AWSConfig(region="us-west-2")

"""
  get_porotomo_files(startdate; enddate=endate, filetype=filetype,orientation=orientation)

Query PoroTomo file availability on AWS. 

# Arguments
- `startdate::TimeType`: Starting time to query PoroTomo files. 

# Keywords 
- `enddate`: Ending time to query PoroTomo files. 
- `filetype`: Return file type. "SEG-Y" is only currently supported. 
- `orientation`: DAS orientation, e.g. "H" for horizontal and "V" for vertical. 
"""
function get_porotomo_files(
    startdate::TimeType;
    enddate=nothing,
    filetype="SEG-Y",
    orientation="H",
)
    if isnothing(enddate)
        enddate = Date(startdate + Day(1))
    end
    @assert startdate <= enddate 
    @assert startdate > Date(2016,3,8)
    @assert enddate < Date(2016,3,27)
    orientation = uppercase(orientation)

    # get filelist for each date
    filelist = Array{String}(undef,0)
    daterange = Date(startdate):Day(1):Date(enddate)
    for d in daterange
        datestr = Dates.format(d,"yyyymmdd")
        path = "$filetype/DAS$orientation/$datestr/"
        dlist = s3_list_objects("nrel-pds-porotomo",path)
        append!(filelist,dlist)
    end

    # cull by datetime 
    times = Array{DateTime}(undef,length(filelist))
    for ii = 1:length(filelist)
        datestr = "20"*String(split(basename(replace(filelist[ii],".sgy"=>"")),"_")[3])
        times[ii] = DateTime(datestr,"yyyymmddHHMMSS")
    end
    ind = findall(startdate .<= times .<= enddate)
    return filelist[ind]
end

function s3_list_objects(bucket,path)
    s3dict = S3.list_objects_v2(bucket,Dict("prefix"=>path),aws_config=aws_config)
    if haskey(s3dict,"Contents")
        keycount = parse(Int,s3dict["KeyCount"])
        filelist = Array{String}(undef,keycount)
        for ii = 1:keycount 
            filelist[ii] = s3dict["Contents"][ii]["Key"]
        end

        if s3dict["IsTruncated"] == "true"
            while s3dict["IsTruncated"] == "true"
                token = s3dict["NextContinuationToken"]
                s3dict = S3.list_objects_v2(
                    bucket,
                    Dict("prefix"=>path,"continuation-token"=>token),
                    aws_config=aws_config,
                )
                keycount = parse(Int,s3dict["KeyCount"])
                newfiles = Array{String}(undef,keycount)
                for ii = 1:keycount 
                    newfiles[ii] = s3dict["Contents"][ii]["Key"]
                end
                append!(filelist,newfiles)
            end
        end
    else
        ErrorException("No files for bucket $bucket path $path")
    end
    return filelist
end

"""
  porotomo_stream(path)

Stream file from AWS to SeisIO.NodalData structure. 

# Arguments 
- `path`: Filepath in `nrel-pds-porotomo` bucket to pull from S3. 

# Examples
```julia-repl
julia> N = porotomo_stream("SEG-Y/DASH/20160325/PoroTomo_iDAS16043_160325235445.sgy")
NodalData with 8721 channels (4 shown)
```
"""
function porotomo_stream(path)
    stream = S3.get_object("nrel-pds-porotomo",path,aws_config=aws_config)
    return stream_nodal_segy(path,IOBuffer(stream))
end


"""
  porotomo_download(path,outdir)

# Arguments
- `path`: Filepath in `nrel-pds-porotomo` bucket to download from S3.
- `OUTDIR::String`: The output directory on EC2 instance.
"""
function porotomo_download(path,OUTDIR)

    OUTDIR = expanduser(OUTDIR)
	outfile = joinpath(OUTDIR,path) 
	filedir = dirname(outfile)
	if !isdir(filedir)
	    mkpath(filedir)
    end
    stream = S3.get_object("nrel-pds-porotomo",path,aws_config=aws_config)
    
    open(outfile, "w") do file
        write(file, stream)
    end
    return nothing
end

function stream_nodal_segy(fname,f)

    chans = Int64[]  
    nn = "N0"
    s = "0001-01-01T00:00:00" 
    t = "9999-12-31T12:59:59"
  
    # Preprocessing
    (d0, d1) = SeisIO.parsetimewin(s, t)
    t0 = DateTime(d0).instant.periods.value*1000 - SeisIO.dtconst
    trace_fh = Array{Int16, 1}(undef, 3)
    shorts  = getfield(SeisIO.BUF, :int16_buf)
    fhd = Dict{String,Any}()
    # ww = Array{String, 1}(undef, 0)
  
    # ==========================================================================
    # Read file header
    fhd["filehdr"]  = SeisIO.fastread(f, 3200)
    fhd["jobid"]    = bswap(SeisIO.fastread(f, Int32))
    fhd["lineid"]   = bswap(SeisIO.fastread(f, Int32))
    fhd["reelid"]   = bswap(SeisIO.fastread(f, Int32))
    SeisIO.fast_readbytes!(f, SeisIO.BUF.buf, 48)
    SeisIO.fillx_i16_be!(shorts, SeisIO.BUF.buf, 24, 0)
    SeisIO.fastskip(f, 240)
    for i in 25:27
      shorts[i] = read(f, Int16)
    end
    SeisIO.fastskip(f, 94)
  
    # check endianness; can be inconsistent; 0x0400 is a kludge
    # (stands for SEG Y rev 10.24, current is rev 2.0)
    # if (unsigned(shorts[25]) > 0x0400) || (shorts[26] < zero(Int16)) || (shorts[26] > one(Int16)) || (shorts[27] < zero(Int16))
    #   shorts[25:27] .= bswap.(shorts[25:27])
    #   push!(ww, "Inconsistent file header endianness")
    # end
    # fastskip(f, 94)
  
    # Process
    nh = max(zero(Int16), getindex(shorts, 27))
    fhd["exthdr"] = Array{String,1}(undef, nh)
    [fhd["exthdr"][i] = SeisIO.fastread(f, 3200) for i in 1:nh]
    for (j,i) in enumerate(String[ "ntr", "naux", "filedt", "origdt", "filenx",
                                   "orignx", "fmt", "cdpfold", "trasort", "vsum",
                                   "swst", "swen0", "swlen", "swtyp", "tapnum",
                                   "swtapst", "swtapen", "taptyp", "corrtra", "bgainrec",
                                   "amprec", "msys", "zupdn", "vibpol", "segyver",
                                   "isfixed", "n_exthdr" ])
      fhd[i] = shorts[j]
    end
  
    nt = getindex(shorts, 1)
    trace_fh[1] = getindex(shorts,3)
    trace_fh[2] = getindex(shorts,5)
    trace_fh[3] = getindex(shorts,7)
  
    # ==========================================================================
    # initialize NodalData container; set variables
    if isempty(chans)
      chans = 1:nt
    end
    fs = SeisIO.sμ / Float64(trace_fh[1])
    data = Array{Float32, 2}(undef, trace_fh[2], nt)
    S = NodalData(data, fhd, chans, t0)
    net = nn * "."
    cha = string("..O", getbandcode(fs), "0")
  
    # ==========================================================================
    # Read traces
    j = 0
  
    for i = 1:nt
      C = SeisIO.do_trace(f, false, true, 0x00, true, trace_fh)
      if i in chans
        j += 1
        S.id[j]       = string(net, lpad(i, 5, '0'), cha)
        S.name[j]     = string(C.misc["rec_no"], "_", i)
        S.fs[j]       = C.fs
        S.gain[j]     = C.gain
        S.misc[j]     = C.misc
        S.t[j]        = C.t
        S.data[:, j] .= C.x
      end
    end
  
    # TO DO: actually use s, t here
  
    # ==========================================================================
    # Cleanup
    close(f)
    resize!(SeisIO.BUF.buf, 65535)
    fill!(S.fs, fs)
    fill!(S.src, fname)
    fill!(S.units, "m/m")
  
    # Output warnings to STDOUT
    # if !isempty(ww)
    #   for i = 1:length(ww)
    #     @warn(ww[i])
    #   end
    # end
    # S.info["Warnings"] = ww
    return S
end

end
