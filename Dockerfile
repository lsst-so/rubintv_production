
ARG STACK_TAG="w_2026_18"
# For USDF, UID=17951
# For summit, UID=GID=73006?


FROM ghcr.io/lsst/scipipe:al9-${STACK_TAG}

ENV UID=73006
ENV GID=73006

ARG drp_pipe_ref="w.2026.18"
ARG summit_utils_ref="3ae001d1e01bd65a4727e27bdbe5ee1fa0720687"
ARG summit_extras_ref="w.2026.18"
ARG ts_wep_ref="5107292b"
ARG donut_viz_ref="18ea94d"
ARG tarts_ref="fa6acd3"
ARG ts_ofc_ref="5245ded"
ARG ts_config_mttcs_ref="ad3ef1b"

ARG USER=saluser
ENV USER=${USER}
ENV WORKDIR=/opt/lsst/software/stack

USER root

# Create user and group
RUN if [ ${UID} -eq 1000 ] && [ ${GID} -eq 1000 ]; then  \
        echo "Renaming lsst to saluser" && \
        usermod -l saluser lsst && \
        usermod -d /home/saluser -m saluser ; \
    else \
        groupadd --gid ${GID} saluser && \
        adduser -u ${UID} -m -g ${GID} -s /bin/bash saluser ; \
    fi

COPY checkout_repo.sh /home/saluser/.checkout_repo.sh

RUN chown saluser:saluser /home/saluser/.checkout_repo.sh && \
    chmod a+x /home/saluser/.checkout_repo.sh

RUN mkdir -p /repos && \
    chmod a+rw /repos && \
    chown saluser:saluser /repos

RUN yum install -y \
      nano \
      mesa-libGL-devel \
      rsync \
      tmux \
  && yum clean all \
  && rm -rf /var/cache/yum

USER lsst

RUN source ${WORKDIR}/loadLSST.bash && \
    conda config --set solver libmamba && \
    conda install -y \
    -c conda-forge \
    # lsstts channel required for ts-ofc
    -c lsstts \
    rubin-env-rsp \
    redis-py \
    batoid \
    danish=1.0.0 \
    rubin-libradtran \
    timm \
    peft \
    && conda clean -afy

USER saluser

RUN source ${WORKDIR}/loadLSST.bash && \
    pip install google-cloud-storage \
    lsst-efd-client \
    pytorch_lightning \
    easyocr \
    sentry-sdk \
    && rm -rf ~/.cache/pip

WORKDIR /repos

# Clone all repos
RUN git clone https://github.com/lsst-sitcom/summit_utils.git && \
    git clone https://github.com/lsst-sitcom/summit_extras.git && \
    git clone https://github.com/lsst-sitcom/rubintv_production.git && \
    git clone https://github.com/lsst-ts/rubintv_analysis_service.git && \
    git clone https://github.com/lsst-ts/ts_wep.git && \
    git clone https://github.com/lsst-ts/ts_ofc.git && \
    git clone https://github.com/lsst-ts/ts_config_mttcs.git && \
    git clone https://github.com/lsst-ts/donut_viz.git && \
    git clone https://github.com/PetchMa/TARTS.git && \
    git clone https://github.com/lsst/drp_pipe.git


WORKDIR /repos/drp_pipe

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${drp_pipe_ref} && \
    eups declare -r . drp_pipe -t saluser && \
    setup drp_pipe -t saluser && \
    scons version

WORKDIR /repos/summit_utils


RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${summit_utils_ref} && \
    eups declare -r . summit_utils -t saluser && \
    setup summit_utils -t saluser && \
    scons version

WORKDIR /repos/summit_extras


RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${summit_extras_ref} && \
    eups declare -r . summit_extras -t saluser && \
    setup summit_extras -t saluser && \
    scons version


WORKDIR /repos/rubintv_analysis_service

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh main && \
    eups declare -r . rubintv_analysis_service -t saluser && \
    setup rubintv_analysis_service  -t saluser && \
    scons version

WORKDIR /repos/ts_wep

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${ts_wep_ref} && \
    eups declare -r . ts_wep -t saluser && \
    setup ts_wep -t saluser && \
    scons version

WORKDIR /repos/ts_ofc

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${ts_ofc_ref} && \
    eups declare -r . ts_ofc -t saluser && \
    setup ts_ofc -t saluser && \
    scons version

WORKDIR /repos/ts_config_mttcs

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${ts_config_mttcs_ref} && \
    eups declare -r . ts_config_mttcs -t saluser && \
    setup ts_config_mttcs -t saluser

WORKDIR /repos/donut_viz

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${donut_viz_ref} && \
    eups declare -r . donut_viz -t saluser && \
    setup donut_viz -t saluser && \
    scons version

WORKDIR /repos/TARTS

RUN source ${WORKDIR}/loadLSST.bash && \
    /home/saluser/.checkout_repo.sh ${tarts_ref} && \
    eups declare -r . tarts -t saluser && \
    setup tarts -t saluser

WORKDIR /repos/rubintv_production

COPY . /repos/rubintv_production


USER root
RUN chown -R ${UID}:${GID} /repos/rubintv_production

USER saluser

RUN git remote set-url origin https://github.com/lsst-sitcom/rubintv_production.git

RUN source ${WORKDIR}/loadLSST.bash && \
    eups declare -r . rubintv_production -t saluser && \
    setup lsst_distrib && \
    setup atmospec -j -t saluser && \
    setup daf_butler -j -t saluser && \
    setup pipe_base -j -t saluser && \
    setup summit_utils -j -t saluser && \
    setup summit_extras -j -t saluser && \
    setup rubintv_production -j -t saluser && \
    setup obs_lsst -j -t saluser && \
    scons version


ENV RUN_ARG="-v"
ENV OPENBLAS_NUM_THREADS=1
ENV GOTO_NUM_THREADS=1
ENV OMP_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV MKL_DOMAIN_NUM_THREADS=1
ENV MPI_NUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1
ENV NUMEXPR_MAX_THREADS=1
ENV MPLBACKEND=Agg

COPY startup.sh /repos/.startup.sh

USER root

RUN chown saluser:saluser /repos/.startup.sh && \
    chmod a+x /repos/.startup.sh && \
    chmod -R a+w /repos && \
    chmod a+rx /home/saluser && \
    rm -rf /home/saluser/.eups/_caches_ && \
    chmod a+w /home/saluser/.eups && \
    chmod a+rwx /tmp

RUN git config --system --add safe.directory /repos/drp_pipe && \
    git config --system --add safe.directory /repos/summit_utils && \
    git config --system --add safe.directory /repos/summit_extras && \
    git config --system --add safe.directory /repos/rubintv_production && \
    git config --system --add safe.directory /repos/rubintv_analysis_service && \
    git config --system --add safe.directory /repos/ts_wep && \
    git config --system --add safe.directory /repos/ts_ofc && \
    git config --system --add safe.directory /repos/ts_config_mttcs && \
    git config --system --add safe.directory /repos/donut_viz && \
    git config --system --add safe.directory /repos/TARTS

USER saluser
ENV USER=saluser
ENV SHELL=/bin/bash
ENV EUPS_USERDATA=/home/saluser/.eups
ENV MPLCONFIGDIR=/tmp

# Spectractor uses numba caching.
ENV NUMBA_CACHE_DIR=/tmp/numba_cache


WORKDIR /repos/rubintv_production/scripts

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["/repos/.startup.sh"]
